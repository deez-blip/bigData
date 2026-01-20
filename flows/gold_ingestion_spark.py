from datetime import datetime
from typing import Tuple
import argparse

from pyspark.sql import DataFrame, SparkSession, Window
from pyspark.sql import functions as F


# -----------------------------
# Spark Session (cluster + MinIO)
# -----------------------------

def get_spark_session(app_name: str = "GoldSpark") -> SparkSession:
    """Crée une SparkSession configurée pour Spark Standalone + MinIO (S3A)."""

    spark = (
        SparkSession.builder.appName(app_name)
        .master("spark://spark-master:7077")
        # --- MinIO (S3A) ---
        .config("spark.hadoop.fs.s3a.endpoint", "http://minio:9000")
        .config("spark.hadoop.fs.s3a.access.key", "minioadmin")
        .config("spark.hadoop.fs.s3a.secret.key", "minioadmin")
        .config("spark.hadoop.fs.s3a.path.style.access", "true")
        .config("spark.hadoop.fs.s3a.connection.ssl.enabled", "false")
        .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
        .getOrCreate()
    )

    spark.sparkContext.setLogLevel("WARN")
    return spark


# -----------------------------
# Paths helpers (Silver/Gold)
# -----------------------------

def silver_path(run_id: str, object_name: str) -> str:
    # Spark lit un "dataset" CSV depuis un dossier (ex: .../clients_clean.csv/part-0000...)
    return f"s3a://silver/runs/{run_id}/{object_name}"


def gold_path(run_id: str, object_name: str) -> str:
    # Spark écrit un "dataset" CSV dans un dossier
    return f"s3a://gold/runs/{run_id}/{object_name}"


def read_silver_csv(spark: SparkSession, run_id: str, object_name: str) -> DataFrame:
    return spark.read.option("header", True).csv(silver_path(run_id, object_name))


def write_gold_csv(df: DataFrame, run_id: str, object_name: str) -> str:
    out = gold_path(run_id, object_name)
    (
        df.coalesce(1)
        .write.mode("overwrite")
        .option("header", True)
        .csv(out)
    )
    return object_name


# -----------------------------
# Gold transformations (Spark)
# -----------------------------

def build_dimensions(
    clients_clean: DataFrame, achats_clean: DataFrame
) -> Tuple[DataFrame, DataFrame, DataFrame]:
    """Construit les dimensions : clients / produits / date."""

    # Dim Clients
    dim_clients = (
        clients_clean.select(
            F.col("id_client").cast("int").alias("id_client"),
            F.trim(F.col("nom").cast("string")).alias("nom"),
            F.trim(F.col("email").cast("string")).alias("email"),
            F.trim(F.col("pays").cast("string")).alias("pays"),
            F.col("date_inscription").cast("string").alias("date_inscription"),
        )
        .dropDuplicates(["id_client"])
    )

    # Dim Produits : distinct produits + id_produit
    produits_distinct = (
        achats_clean.select(F.trim(F.col("produit").cast("string")).alias("produit"))
        .dropna(subset=["produit"])
        .dropDuplicates(["produit"])
    )

    w_prod = Window.orderBy(F.col("produit").asc())
    dim_produits = produits_distinct.withColumn("id_produit", F.row_number().over(w_prod))
    dim_produits = dim_produits.select("id_produit", "produit")

    # Dim Date : basé sur date_achat
    dates = (
        achats_clean.select(F.to_date(F.col("date_achat").cast("string"), "yyyy-MM-dd").alias("date"))
        .dropna(subset=["date"])
        .dropDuplicates(["date"])
    )

    dim_date = (
        dates.withColumn("year", F.year("date"))
        .withColumn("month", F.month("date"))
        .withColumn("month_name", F.date_format("date", "MMMM"))
        .withColumn("week", F.weekofyear("date"))
        .withColumn("day", F.dayofmonth("date"))
        .withColumn("day_name", F.date_format("date", "EEEE"))
        .withColumn("quarter", F.quarter("date"))
        .withColumn("date", F.date_format("date", "yyyy-MM-dd"))
        .select("date", "year", "month", "month_name", "week", "day", "day_name", "quarter")
        .orderBy("date")
    )

    return dim_clients, dim_produits, dim_date


def build_fact_achats(
    achats_clean: DataFrame, dim_clients: DataFrame, dim_produits: DataFrame
) -> DataFrame:
    """Construit la fact table achats (grain : 1 ligne = 1 achat)."""

    fact = (
        achats_clean.select(
            F.col("id_achat").cast("int").alias("id_achat"),
            F.col("id_client").cast("int").alias("id_client"),
            F.trim(F.col("produit").cast("string")).alias("produit"),
            F.to_date(F.col("date_achat").cast("string"), "yyyy-MM-dd").alias("date_achat"),
            F.col("montant").cast("double").alias("montant"),
        )
    )

    fact = fact.join(dim_produits, on="produit", how="left")
    fact = fact.join(dim_clients.select("id_client", "pays"), on="id_client", how="left")

    fact = (
        fact.withColumn("date_achat", F.date_format(F.col("date_achat"), "yyyy-MM-dd"))
        .select("id_achat", "id_client", "id_produit", "produit", "date_achat", "montant", "pays")
    )

    return fact


def build_promo_tables(promos_clean: DataFrame, dim_clients: DataFrame) -> Tuple[DataFrame, DataFrame]:
    """Construit dim_promos + fact_promos (grain : 1 ligne = 1 promo attribuée à 1 client)."""

    promos_df = (
        promos_clean.select(
            F.col("promo_id").cast("int").alias("promo_id"),
            F.col("id_client").cast("int").alias("id_client"),
            F.trim(F.col("promo_name").cast("string")).alias("promo_name"),
            F.col("promo_value").cast("double").alias("promo_value"),
        )
    )

    dim_promos = promos_df.select("promo_id", "promo_name", "promo_value").dropDuplicates(["promo_id"])

    fact_promos = promos_df.join(dim_clients.select("id_client", "pays"), on="id_client", how="left")
    fact_promos = fact_promos.select("promo_id", "id_client", "promo_name", "promo_value", "pays")

    return dim_promos, fact_promos


def compute_kpis(fact_achats: DataFrame) -> Tuple[DataFrame, DataFrame, DataFrame]:
    """Calcule KPIs globaux + par pays + par produit."""

    # KPI global
    kpi_global = (
        fact_achats.agg(
            F.round(F.sum("montant"), 2).alias("total_revenue"),
            F.countDistinct("id_achat").alias("total_orders"),
            F.countDistinct("id_client").alias("unique_customers"),
        )
        .withColumn(
            "avg_order_value",
            F.when(F.col("total_orders") > 0, F.round(F.col("total_revenue") / F.col("total_orders"), 2)).otherwise(F.lit(0.0)),
        )
        .withColumn("generated_at", F.lit(datetime.now().isoformat(timespec="seconds")))
    )

    # KPI par pays
    kpi_by_country = (
        fact_achats.groupBy("pays")
        .agg(
            F.round(F.sum("montant"), 2).alias("revenue"),
            F.countDistinct("id_achat").alias("orders"),
            F.countDistinct("id_client").alias("customers"),
        )
        .withColumn(
            "avg_order_value",
            F.when(F.col("orders") > 0, F.round(F.col("revenue") / F.col("orders"), 2)).otherwise(F.lit(0.0)),
        )
        .orderBy(F.col("revenue").desc())
    )

    # KPI par produit
    kpi_by_product = (
        fact_achats.groupBy("id_produit", "produit")
        .agg(
            F.round(F.sum("montant"), 2).alias("revenue"),
            F.countDistinct("id_achat").alias("orders"),
            F.countDistinct("id_client").alias("customers"),
        )
        .withColumn(
            "avg_order_value",
            F.when(F.col("orders") > 0, F.round(F.col("revenue") / F.col("orders"), 2)).otherwise(F.lit(0.0)),
        )
        .orderBy(F.col("revenue").desc())
    )

    return kpi_global, kpi_by_country, kpi_by_product


def compute_promo_kpis(fact_promos: DataFrame) -> Tuple[DataFrame, DataFrame, DataFrame]:
    """Calcule des KPIs promos : global + par promo + par pays."""

    promo_kpi_global = (
        fact_promos.agg(
            F.count(F.lit(1)).alias("total_promos"),
            F.countDistinct("id_client").alias("unique_clients_with_promo"),
            F.countDistinct("promo_id").alias("unique_promo_ids"),
            F.round(F.avg("promo_value"), 2).alias("avg_promo_value"),
        )
        .withColumn("generated_at", F.lit(datetime.now().isoformat(timespec="seconds")))
    )

    promo_kpi_by_name = (
        fact_promos.groupBy("promo_id", "promo_name")
        .agg(
            F.count(F.lit(1)).alias("promos_count"),
            F.countDistinct("id_client").alias("clients"),
            F.round(F.avg("promo_value"), 2).alias("avg_value"),
        )
        .orderBy(F.col("promos_count").desc())
    )

    promo_kpi_by_country = (
        fact_promos.groupBy("pays")
        .agg(
            F.count(F.lit(1)).alias("promos_count"),
            F.countDistinct("id_client").alias("clients"),
            F.round(F.avg("promo_value"), 2).alias("avg_value"),
        )
        .orderBy(F.col("promos_count").desc())
    )

    return promo_kpi_global, promo_kpi_by_name, promo_kpi_by_country


def aggregate_temporal(fact_achats: DataFrame) -> Tuple[DataFrame, DataFrame, DataFrame]:
    """Pré-calcule agrégations jour / semaine / mois."""

    base = fact_achats.withColumn("date", F.to_date(F.col("date_achat"), "yyyy-MM-dd"))

    # Day
    agg_day = (
        base.groupBy(F.date_format("date", "yyyy-MM-dd").alias("day"))
        .agg(
            F.round(F.sum("montant"), 2).alias("revenue"),
            F.countDistinct("id_achat").alias("orders"),
            F.countDistinct("id_client").alias("customers"),
        )
        .orderBy("day")
    )

    # ISO Week
    agg_week = (
        base.withColumn("year", F.year("date"))
        .withColumn("week", F.weekofyear("date"))
        .groupBy("year", "week")
        .agg(
            F.round(F.sum("montant"), 2).alias("revenue"),
            F.countDistinct("id_achat").alias("orders"),
            F.countDistinct("id_client").alias("customers"),
        )
        .orderBy("year", "week")
    )

    # Month
    agg_month = (
        base.withColumn("month", F.date_format("date", "yyyy-MM"))
        .groupBy("month")
        .agg(
            F.round(F.sum("montant"), 2).alias("revenue"),
            F.countDistinct("id_achat").alias("orders"),
            F.countDistinct("id_client").alias("customers"),
        )
        .orderBy("month")
    )

    return agg_day, agg_week, agg_month


# -----------------------------
# latest_run.json pointer (best effort)
# -----------------------------

def write_latest_run_pointer_best_effort(run_id: str) -> str:
    """Ecrit gold/latest_run.json si possible (sinon fallback dataset Spark)."""

    payload = {
        "run_id": run_id,
        "gold_prefix": f"runs/{run_id}/",
        "generated_at": datetime.now().isoformat(timespec="seconds"),
    }

    # 1) Tentative via lib MinIO (idéal : fichier unique)
    try:
        import json
        from io import BytesIO
        from minio import Minio

        client = Minio(
            "minio:9000",
            access_key="minioadmin",
            secret_key="minioadmin",
            secure=False,
        )

        data = json.dumps(payload, ensure_ascii=False, indent=2).encode("utf-8")
        client.put_object(
            "gold",
            "latest_run.json",
            BytesIO(data),
            length=len(data),
            content_type="application/json",
        )

        return "latest_run.json"

    except Exception:
        # 2) Fallback : on écrit un dataset Spark (ça fera un dossier)
        try:
            spark = SparkSession.getActiveSession() or get_spark_session("GoldPointerFallback")
            df = spark.createDataFrame([payload])
            df.coalesce(1).write.mode("overwrite").json("s3a://gold/latest_run.json")
            return "latest_run.json (spark_dataset_fallback)"
        except Exception:
            return "latest_run.json (failed)"


# -----------------------------
# Gold pipeline (Spark)
# -----------------------------

def gold_ingestion_spark(run_id: str) -> dict:
    """Gold Spark : lit Silver -> dims/facts -> KPIs/aggs -> écrit Gold (historisé par run_id)."""

    spark = get_spark_session("GoldIngestionSpark")

    try:
        # 1) Read from Silver (datasets CSV)
        clients_clean = read_silver_csv(spark, run_id, "clients_clean.csv")
        achats_clean = read_silver_csv(spark, run_id, "achats_clean.csv")
        promos_clean = read_silver_csv(spark, run_id, "promo_clean.csv")

        # 2) Build star schema tables
        dim_clients, dim_produits, dim_date = build_dimensions(clients_clean, achats_clean)
        fact_achats = build_fact_achats(achats_clean, dim_clients, dim_produits)
        dim_promos, fact_promos = build_promo_tables(promos_clean, dim_clients)

        # 3) KPIs
        kpi_global, kpi_by_country, kpi_by_product = compute_kpis(fact_achats)
        promo_kpi_global, promo_kpi_by_name, promo_kpi_by_country = compute_promo_kpis(fact_promos)

        # 4) Temporal aggregations
        agg_day, agg_week, agg_month = aggregate_temporal(fact_achats)

        # 5) Write outputs to Gold
        outputs = {
            "dim_clients": write_gold_csv(dim_clients, run_id, "dim_clients.csv"),
            "dim_produits": write_gold_csv(dim_produits, run_id, "dim_produits.csv"),
            "dim_date": write_gold_csv(dim_date, run_id, "dim_date.csv"),
            "fact_achats": write_gold_csv(fact_achats, run_id, "fact_achats.csv"),
            "dim_promos": write_gold_csv(dim_promos, run_id, "dim_promos.csv"),
            "fact_promos": write_gold_csv(fact_promos, run_id, "fact_promos.csv"),
            "kpi_global": write_gold_csv(kpi_global, run_id, "kpi_global.csv"),
            "kpi_by_country": write_gold_csv(kpi_by_country, run_id, "kpi_by_country.csv"),
            "kpi_by_product": write_gold_csv(kpi_by_product, run_id, "kpi_by_product.csv"),
            "promo_kpi_global": write_gold_csv(promo_kpi_global, run_id, "promo_kpi_global.csv"),
            "promo_kpi_by_name": write_gold_csv(promo_kpi_by_name, run_id, "promo_kpi_by_name.csv"),
            "promo_kpi_by_country": write_gold_csv(promo_kpi_by_country, run_id, "promo_kpi_by_country.csv"),
            "agg_day": write_gold_csv(agg_day, run_id, "agg_day.csv"),
            "agg_week": write_gold_csv(agg_week, run_id, "agg_week.csv"),
            "agg_month": write_gold_csv(agg_month, run_id, "agg_month.csv"),
        }

        latest_pointer = write_latest_run_pointer_best_effort(run_id)

        report = {
            "run_id": run_id,
            "outputs": {**outputs, "latest_run_pointer": latest_pointer},
            "meta": {
                "bucket_silver": "silver",
                "bucket_gold": "gold",
                "written_prefix": f"runs/{run_id}/",
                "run_at": datetime.now().isoformat(timespec="seconds"),
            },
            "stats": {
                "dim_clients_rows": int(dim_clients.count()),
                "fact_achats_rows": int(fact_achats.count()),
                "fact_promos_rows": int(fact_promos.count()),
            },
        }

        return report

    finally:
        spark.stop()


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Gold ingestion Spark")
    parser.add_argument("--run-id", required=True, help="Run id (ex: 20260120_101037)")
    args = parser.parse_args()

    result = gold_ingestion_spark(args.run_id)
    print("\n✅ Gold Spark ingestion completed")
    print(result)