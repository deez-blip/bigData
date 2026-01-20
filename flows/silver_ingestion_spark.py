from datetime import datetime
from typing import Dict, Tuple
import argparse

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F


# -----------------------------
# Référentiels (Silver)
# -----------------------------
COUNTRIES = [
    "France",
    "Germany",
    "Spain",
    "Italy",
    "Belgium",
    "Netherland",
    "Switzerland",
    "Portugal",
]

PRODUITS = [
    "Laptop",
    "Smartphone",
    "Tablet",
    "Headphones",
    "Smartwatch",
    "Camera",
    "Printer",
    "Monitor",
]

PROMO_NAMES = [
    "WELCOME10",
    "SUMMER15",
    "BLACKFRIDAY20",
    "FREESHIP",
    "VIP25",
    "FLASH5",
    "STUDENT12",
]


# -----------------------------
# Spark Session (cluster + MinIO)
# -----------------------------

def get_spark_session(app_name: str = "SilverSpark") -> SparkSession:
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

    # Logs moins bruyants
    spark.sparkContext.setLogLevel("WARN")

    return spark


# -----------------------------
# IO helpers (Bronze/Silver)
# -----------------------------

def bronze_path(run_id: str, filename: str) -> str:
    return f"s3a://bronze/runs/{run_id}/{filename}"


def silver_object_path(run_id: str, object_name: str) -> str:
    # Spark écrit dans un dossier (ex: .../clients_clean.csv/part-0000...)
    return f"s3a://silver/runs/{run_id}/{object_name}"


def read_bronze_csv(spark: SparkSession, run_id: str, filename: str) -> DataFrame:
    return spark.read.option("header", True).csv(bronze_path(run_id, filename))


def write_silver_csv(df: DataFrame, run_id: str, object_name: str) -> str:
    out = silver_object_path(run_id, object_name)
    (
        df.coalesce(1)
        .write.mode("overwrite")
        .option("header", True)
        .csv(out)
    )
    return object_name


# -----------------------------
# Cleaning rules (Spark)
# -----------------------------

def clean_clients_df_spark(df: DataFrame) -> Tuple[DataFrame, DataFrame, Dict[str, int]]:
    """Nettoie/valide la table clients (Spark) et renvoie (clean, rejected, report)."""

    report: Dict[str, int] = {}

    # Normalisations + casts
    clients = (
        df.withColumn("id_client", F.col("id_client").cast("int"))
        .withColumn("nom", F.trim(F.col("nom").cast("string")))
        .withColumn("email", F.trim(F.col("email").cast("string")))
        .withColumn("pays", F.trim(F.col("pays").cast("string")))
        .withColumn("date_inscription", F.to_date(F.col("date_inscription"), "yyyy-MM-dd"))
    )

    today = F.current_date()

    mask_id_ok = F.col("id_client").isNotNull()
    mask_nom_ok = F.length(F.col("nom")) > 0
    mask_email_ok = F.col("email").rlike(r"^[^@\s]+@[^@\s]+\.[^@\s]+$")
    mask_date_ok = F.col("date_inscription").isNotNull() & (F.col("date_inscription") <= today)
    mask_country_ok = F.col("pays").isin(COUNTRIES)

    valid_cond = mask_id_ok & mask_nom_ok & mask_email_ok & mask_date_ok & mask_country_ok

    rejected = clients.filter(~valid_cond)
    clean = clients.filter(valid_cond)

    before_dedup = clean.count()
    clean = clean.dropDuplicates(["id_client"])
    after_dedup = clean.count()

    # Standardisation date (ISO)
    clean = clean.withColumn("date_inscription", F.date_format(F.col("date_inscription"), "yyyy-MM-dd"))

    report["clients_total"] = int(clients.count())
    report["clients_clean"] = int(after_dedup)
    report["clients_rejected"] = int(rejected.count())
    report["clients_deduplicated"] = int(before_dedup - after_dedup)

    # Stats debug (approximations utiles)
    report["clients_rejected_missing_id"] = int(clients.filter(~mask_id_ok).count())
    report["clients_rejected_missing_nom"] = int(clients.filter(~mask_nom_ok).count())
    report["clients_rejected_bad_email"] = int(clients.filter(~mask_email_ok).count())
    report["clients_rejected_bad_date"] = int(clients.filter(~mask_date_ok).count())
    report["clients_rejected_bad_country"] = int(clients.filter(~mask_country_ok).count())

    return clean, rejected, report


def clean_achats_df_spark(df: DataFrame, clients_clean: DataFrame) -> Tuple[DataFrame, DataFrame, Dict[str, int]]:
    """Nettoie/valide la table achats (Spark) et renvoie (clean, rejected, report)."""

    report: Dict[str, int] = {}

    achats = (
        df.withColumn("id_achat", F.col("id_achat").cast("int"))
        .withColumn("id_client", F.col("id_client").cast("int"))
        .withColumn("produit", F.trim(F.col("produit").cast("string")))
        .withColumn("montant", F.col("montant").cast("double"))
        .withColumn("date_achat", F.to_date(F.col("date_achat"), "yyyy-MM-dd"))
    )

    today = F.current_date()

    mask_id_achat_ok = F.col("id_achat").isNotNull()
    mask_id_client_ok = F.col("id_client").isNotNull()
    mask_date_ok = F.col("date_achat").isNotNull() & (F.col("date_achat") <= today)
    mask_montant_ok = F.col("montant").isNotNull() & (F.col("montant") > 0) & (F.col("montant") <= 5000)
    mask_produit_ok = F.col("produit").isin(PRODUITS)

    base_valid_cond = mask_id_achat_ok & mask_id_client_ok & mask_date_ok & mask_montant_ok & mask_produit_ok

    # D'abord : valide sans FK
    valid_no_fk = achats.filter(base_valid_cond)
    rejected_other = achats.filter(~base_valid_cond)

    # FK : id_client doit exister dans clients_clean
    client_ids = clients_clean.select("id_client").dropDuplicates(["id_client"])

    valid_fk = valid_no_fk.join(F.broadcast(client_ids), on="id_client", how="inner")
    rejected_fk = valid_no_fk.join(F.broadcast(client_ids), on="id_client", how="left_anti")

    clean = valid_fk
    rejected = rejected_other.unionByName(rejected_fk).dropDuplicates()

    # Déduplication
    before_exact = clean.count()
    clean = clean.dropDuplicates()  # doublons exacts
    after_exact = clean.count()

    before_id = after_exact
    clean = clean.dropDuplicates(["id_achat"])  # unicité id_achat
    after_id = clean.count()

    # Standardisation date
    clean = clean.withColumn("date_achat", F.date_format(F.col("date_achat"), "yyyy-MM-dd"))

    report["achats_total"] = int(achats.count())
    report["achats_clean"] = int(after_id)
    report["achats_rejected"] = int(rejected.count())

    report["achats_deduplicated_exact"] = int(before_exact - after_exact)
    report["achats_deduplicated_by_id"] = int(before_id - after_id)

    report["achats_rejected_missing_id_achat"] = int(achats.filter(~mask_id_achat_ok).count())
    report["achats_rejected_missing_id_client"] = int(achats.filter(~mask_id_client_ok).count())
    report["achats_rejected_bad_date"] = int(achats.filter(~mask_date_ok).count())
    report["achats_rejected_bad_montant"] = int(achats.filter(~mask_montant_ok).count())
    report["achats_rejected_bad_produit"] = int(achats.filter(~mask_produit_ok).count())
    report["achats_rejected_fk"] = int(rejected_fk.count())

    return clean, rejected, report


def clean_promos_df_spark(df: DataFrame, clients_clean: DataFrame) -> Tuple[DataFrame, DataFrame, Dict[str, int]]:
    """Nettoie/valide la table promos (Spark) et renvoie (clean, rejected, report)."""

    report: Dict[str, int] = {}

    promos = (
        df.withColumn("id_client", F.col("id_client").cast("int"))
        .withColumn("promo_id", F.col("promo_id").cast("int"))
        .withColumn("promo_name", F.trim(F.col("promo_name").cast("string")))
        .withColumn("promo_value", F.col("promo_value").cast("double"))
    )

    mask_client_ok = F.col("id_client").isNotNull()
    mask_promo_id_ok = F.col("promo_id").isNotNull()
    mask_name_ok = F.length(F.col("promo_name")) > 0
    mask_name_ref_ok = F.col("promo_name").isin(PROMO_NAMES)
    mask_value_ok = F.col("promo_value").isNotNull() & (F.col("promo_value") > 0) & (F.col("promo_value") <= 100)

    base_valid_cond = mask_client_ok & mask_promo_id_ok & mask_name_ok & mask_name_ref_ok & mask_value_ok

    valid_no_fk = promos.filter(base_valid_cond)
    rejected_other = promos.filter(~base_valid_cond)

    client_ids = clients_clean.select("id_client").dropDuplicates(["id_client"])

    valid_fk = valid_no_fk.join(F.broadcast(client_ids), on="id_client", how="inner")
    rejected_fk = valid_no_fk.join(F.broadcast(client_ids), on="id_client", how="left_anti")

    clean = valid_fk
    rejected = rejected_other.unionByName(rejected_fk).dropDuplicates()

    # Déduplication
    before_exact = clean.count()
    clean = clean.dropDuplicates()
    after_exact = clean.count()

    before_id = after_exact
    clean = clean.dropDuplicates(["promo_id"])
    after_id = clean.count()

    report["promos_total"] = int(promos.count())
    report["promos_clean"] = int(after_id)
    report["promos_rejected"] = int(rejected.count())

    report["promos_deduplicated_exact"] = int(before_exact - after_exact)
    report["promos_deduplicated_by_id"] = int(before_id - after_id)

    report["promos_rejected_missing_id_client"] = int(promos.filter(~mask_client_ok).count())
    report["promos_rejected_missing_promo_id"] = int(promos.filter(~mask_promo_id_ok).count())
    report["promos_rejected_missing_name"] = int(promos.filter(~mask_name_ok).count())
    report["promos_rejected_bad_name"] = int(promos.filter(~mask_name_ref_ok).count())
    report["promos_rejected_bad_value"] = int(promos.filter(~mask_value_ok).count())
    report["promos_rejected_fk"] = int(rejected_fk.count())

    return clean, rejected, report


# -----------------------------
# Silver pipeline (Spark)
# -----------------------------

def silver_ingestion_spark(run_id: str) -> dict:
    """Pipeline Silver Spark : lit Bronze -> clean/rejected -> écrit Silver (historisé par run_id)."""

    spark = get_spark_session("SilverIngestionSpark")

    try:
        # 1) Read from Bronze
        clients_bronze = read_bronze_csv(spark, run_id, "clients.csv")
        achats_bronze = read_bronze_csv(spark, run_id, "achats.csv")
        promo_bronze = read_bronze_csv(spark, run_id, "promo.csv")

        # 2) Clean
        clients_clean, clients_rejected, clients_report = clean_clients_df_spark(clients_bronze)
        achats_clean, achats_rejected, achats_report = clean_achats_df_spark(achats_bronze, clients_clean)
        promo_clean, promo_rejected, promo_report = clean_promos_df_spark(promo_bronze, clients_clean)

        # 3) Write to Silver (Spark écrit en dossiers)
        out_clients_clean = write_silver_csv(clients_clean, run_id, "clients_clean.csv")
        out_clients_rejected = write_silver_csv(clients_rejected, run_id, "clients_rejected.csv")

        out_achats_clean = write_silver_csv(achats_clean, run_id, "achats_clean.csv")
        out_achats_rejected = write_silver_csv(achats_rejected, run_id, "achats_rejected.csv")

        out_promo_clean = write_silver_csv(promo_clean, run_id, "promo_clean.csv")
        out_promo_rejected = write_silver_csv(promo_rejected, run_id, "promo_rejected.csv")

        # 4) Report
        report = {
            "run_id": run_id,
            "outputs": {
                "clients_clean": out_clients_clean,
                "clients_rejected": out_clients_rejected,
                "achats_clean": out_achats_clean,
                "achats_rejected": out_achats_rejected,
                "promo_clean": out_promo_clean,
                "promo_rejected": out_promo_rejected,
            },
            "quality": {
                **clients_report,
                **achats_report,
                **promo_report,
            },
            "meta": {
                "bucket_bronze": "bronze",
                "bucket_silver": "silver",
                "written_prefix": f"runs/{run_id}/",
                "run_at": datetime.now().isoformat(timespec="seconds"),
            },
        }

        return report

    finally:
        spark.stop()


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Silver ingestion Spark")
    parser.add_argument("--run-id", required=True, help="Run id (ex: 20260120_101037)")
    args = parser.parse_args()

    result = silver_ingestion_spark(args.run_id)
    print("\n✅ Silver Spark ingestion completed")
    print(result)