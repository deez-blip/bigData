from io import BytesIO
from datetime import datetime
from typing import Tuple
import json

import pandas as pd
from prefect import flow, task

from config import BUCKET_SILVER, BUCKET_GOLD, get_minio_client


@task(name="ensure_bucket", retries=3, retry_delay_seconds=2)
def ensure_bucket_exists(bucket_name: str) -> str:
    """Crée le bucket MinIO s'il n'existe pas."""
    client = get_minio_client()
    if not client.bucket_exists(bucket_name):
        client.make_bucket(bucket_name)
    return bucket_name


@task(name="minio_read_csv_silver", retries=3, retry_delay_seconds=2)
def read_csv_from_silver(object_name: str) -> pd.DataFrame:
    """Lit un CSV depuis le bucket SILVER et le charge en DataFrame pandas."""
    client = get_minio_client()
    obj = client.get_object(BUCKET_SILVER, object_name)
    data = obj.read()
    # Bonne pratique : libérer la connexion
    try:
        obj.close()
        obj.release_conn()
    except Exception:
        pass
    return pd.read_csv(BytesIO(data))


@task(name="minio_write_csv_gold", retries=3, retry_delay_seconds=2)
def write_csv_to_gold(df: pd.DataFrame, object_name: str) -> str:
    """Ecrit un DataFrame en CSV dans le bucket GOLD."""
    client = get_minio_client()

    buffer = BytesIO()
    df.to_csv(buffer, index=False)
    buffer.seek(0)

    client.put_object(
        BUCKET_GOLD,
        object_name,
        buffer,
        length=buffer.getbuffer().nbytes,
        content_type="text/csv",
    )
    return object_name


# ---- Write latest_run.json pointer ----
@task(name="write_latest_run_pointer", retries=3, retry_delay_seconds=2)
def write_latest_run_pointer(gold_prefix: str) -> str:
    """Ecrit un fichier pointeur latest_run.json dans le bucket GOLD."""

    # Déduit run_id depuis un prefix type: "runs/<run_id>/"
    run_id = ""
    norm = (gold_prefix or "").strip("/")
    parts = norm.split("/") if norm else []
    if len(parts) >= 2 and parts[0] == "runs":
        run_id = parts[1]

    payload = {
        "run_id": run_id,
        "gold_prefix": gold_prefix,
        "generated_at": datetime.now().isoformat(timespec="seconds"),
    }

    data = json.dumps(payload, ensure_ascii=False, indent=2).encode("utf-8")

    client = get_minio_client()
    client.put_object(
        BUCKET_GOLD,
        "latest_run.json",
        BytesIO(data),
        length=len(data),
        content_type="application/json",
    )

    return "latest_run.json"


@task(name="build_dimensions")
def build_dimensions(
    clients_clean: pd.DataFrame, achats_clean: pd.DataFrame
) -> Tuple[pd.DataFrame, pd.DataFrame, pd.DataFrame]:
    """Construit les dimensions principales : clients / produits / date."""

    # -------------------
    # Dim Clients
    # -------------------
    dim_clients = clients_clean.copy()

    # Normalisation minimale (au cas où)
    dim_clients["id_client"] = pd.to_numeric(dim_clients["id_client"], errors="coerce").astype("Int64")
    for col in ["nom", "email", "pays"]:
        if col in dim_clients.columns:
            dim_clients[col] = dim_clients[col].astype("string").fillna("").str.strip()

    # On garde uniquement les colonnes utiles (dimension = descriptive)
    dim_clients = dim_clients[["id_client", "nom", "email", "pays", "date_inscription"]].drop_duplicates(
        subset=["id_client"], keep="first"
    )

    # -------------------
    # Dim Produits
    # -------------------
    dim_produits = achats_clean[["produit"]].copy()
    dim_produits["produit"] = dim_produits["produit"].astype("string").fillna("").str.strip()
    dim_produits = dim_produits.drop_duplicates().sort_values("produit").reset_index(drop=True)
    dim_produits.insert(0, "id_produit", range(1, len(dim_produits) + 1))

    # -------------------
    # Dim Date
    # -------------------
    # On génère une dimension date à partir des dates d'achat
    date_series = pd.to_datetime(achats_clean["date_achat"], errors="coerce")
    min_date = date_series.min()
    max_date = date_series.max()

    if pd.isna(min_date) or pd.isna(max_date):
        # cas extrême : aucun achat
        dim_date = pd.DataFrame(
            columns=["date", "year", "month", "month_name", "week", "day", "day_name", "quarter"]
        )
    else:
        all_dates = pd.date_range(start=min_date.normalize(), end=max_date.normalize(), freq="D")
        dim_date = pd.DataFrame({"date": all_dates})
        dim_date["year"] = dim_date["date"].dt.year
        dim_date["month"] = dim_date["date"].dt.month
        dim_date["month_name"] = dim_date["date"].dt.month_name()
        dim_date["week"] = dim_date["date"].dt.isocalendar().week.astype(int)
        dim_date["day"] = dim_date["date"].dt.day
        dim_date["day_name"] = dim_date["date"].dt.day_name()
        dim_date["quarter"] = dim_date["date"].dt.quarter

        # clé date en string ISO
        dim_date["date"] = dim_date["date"].dt.strftime("%Y-%m-%d")

    return dim_clients, dim_produits, dim_date


@task(name="build_fact_achats")
def build_fact_achats(
    achats_clean: pd.DataFrame, dim_clients: pd.DataFrame, dim_produits: pd.DataFrame
) -> pd.DataFrame:
    """Construit la table de faits des achats (grain : 1 ligne = 1 achat)."""

    fact = achats_clean.copy()

    # Types
    fact["id_achat"] = pd.to_numeric(fact["id_achat"], errors="coerce").astype("Int64")
    fact["id_client"] = pd.to_numeric(fact["id_client"], errors="coerce").astype("Int64")
    fact["montant"] = pd.to_numeric(fact["montant"], errors="coerce")
    fact["produit"] = fact["produit"].astype("string").fillna("").str.strip()
    fact["date_achat"] = pd.to_datetime(fact["date_achat"], errors="coerce").dt.strftime("%Y-%m-%d")

    # Join dimension produit -> id_produit
    fact = fact.merge(dim_produits, on="produit", how="left")

    # Join dimension client -> pays (utile pour KPI)
    fact = fact.merge(
        dim_clients[["id_client", "pays"]],
        on="id_client",
        how="left",
        suffixes=("", "_client"),
    )

    # Colonnes finales de la fact
    # (on garde produit en clair + id_produit pour le star schema)
    fact = fact[["id_achat", "id_client", "id_produit", "produit", "date_achat", "montant", "pays"]]

    return fact


@task(name="build_promo_tables")
def build_promo_tables(
    promos_clean: pd.DataFrame, dim_clients: pd.DataFrame
) -> Tuple[pd.DataFrame, pd.DataFrame]:
    """Construit dim_promos + fact_promos (grain : 1 ligne = 1 promo attribuée à 1 client)."""

    promos_df = promos_clean.copy()

    # Types
    promos_df["promo_id"] = pd.to_numeric(promos_df["promo_id"], errors="coerce").astype("Int64")
    promos_df["id_client"] = pd.to_numeric(promos_df["id_client"], errors="coerce").astype("Int64")
    promos_df["promo_value"] = pd.to_numeric(promos_df["promo_value"], errors="coerce")
    promos_df["promo_name"] = promos_df["promo_name"].astype("string").fillna("").str.strip()

    # -------------------
    # Dim Promos
    # -------------------
    dim_promos = promos_df[["promo_id", "promo_name", "promo_value"]].drop_duplicates(subset=["promo_id"], keep="first")

    # -------------------
    # Fact Promos
    # -------------------
    # Join client -> pays (utile pour KPI)
    fact_promos = promos_df.merge(
        dim_clients[["id_client", "pays"]],
        on="id_client",
        how="left",
        suffixes=("", "_client"),
    )

    fact_promos = fact_promos[["promo_id", "id_client", "promo_name", "promo_value", "pays"]]

    return dim_promos, fact_promos


@task(name="compute_promo_kpis")
def compute_promo_kpis(fact_promos: pd.DataFrame) -> Tuple[pd.DataFrame, pd.DataFrame, pd.DataFrame]:
    """Calcule des KPIs liés aux promos : globaux + par promo + par pays."""

    total_promos = int(len(fact_promos))
    unique_clients_with_promo = int(fact_promos["id_client"].nunique())
    unique_promo_ids = int(fact_promos["promo_id"].nunique())
    avg_promo_value = float(fact_promos["promo_value"].mean()) if total_promos else 0.0

    promo_kpi_global = pd.DataFrame(
        [
            {
                "total_promos": total_promos,
                "unique_clients_with_promo": unique_clients_with_promo,
                "unique_promo_ids": unique_promo_ids,
                "avg_promo_value": round(avg_promo_value, 2),
                "generated_at": datetime.now().isoformat(timespec="seconds"),
            }
        ]
    )

    promo_kpi_by_name = (
        fact_promos.groupby(["promo_id", "promo_name"], dropna=False)
        .agg(
            promos_count=("promo_id", "count"),
            clients=("id_client", "nunique"),
            avg_value=("promo_value", "mean"),
        )
        .reset_index()
        .sort_values("promos_count", ascending=False)
    )
    promo_kpi_by_name["avg_value"] = promo_kpi_by_name["avg_value"].round(2)

    promo_kpi_by_country = (
        fact_promos.groupby(["pays"], dropna=False)
        .agg(
            promos_count=("promo_id", "count"),
            clients=("id_client", "nunique"),
            avg_value=("promo_value", "mean"),
        )
        .reset_index()
        .sort_values("promos_count", ascending=False)
    )
    promo_kpi_by_country["avg_value"] = promo_kpi_by_country["avg_value"].round(2)

    return promo_kpi_global, promo_kpi_by_name, promo_kpi_by_country


@task(name="compute_kpis")
def compute_kpis(fact_achats: pd.DataFrame) -> Tuple[pd.DataFrame, pd.DataFrame, pd.DataFrame]:
    """Calcule des KPIs globaux + répartitions par pays et par produit."""

    # KPI globaux
    total_revenue = float(fact_achats["montant"].sum())
    total_orders = int(fact_achats["id_achat"].nunique())
    unique_customers = int(fact_achats["id_client"].nunique())
    avg_order_value = float(total_revenue / total_orders) if total_orders else 0.0

    kpis_global = pd.DataFrame(
        [
            {
                "total_revenue": round(total_revenue, 2),
                "total_orders": total_orders,
                "unique_customers": unique_customers,
                "avg_order_value": round(avg_order_value, 2),
                "generated_at": datetime.now().isoformat(timespec="seconds"),
            }
        ]
    )

    # KPI par pays
    kpis_by_country = (
        fact_achats.groupby("pays", dropna=False)
        .agg(revenue=("montant", "sum"), orders=("id_achat", "nunique"), customers=("id_client", "nunique"))
        .reset_index()
    )
    kpis_by_country["avg_order_value"] = (kpis_by_country["revenue"] / kpis_by_country["orders"]).round(2)
    kpis_by_country["revenue"] = kpis_by_country["revenue"].round(2)

    # KPI par produit
    kpis_by_product = (
        fact_achats.groupby(["id_produit", "produit"], dropna=False)
        .agg(revenue=("montant", "sum"), orders=("id_achat", "nunique"), customers=("id_client", "nunique"))
        .reset_index()
        .sort_values("revenue", ascending=False)
    )
    kpis_by_product["avg_order_value"] = (kpis_by_product["revenue"] / kpis_by_product["orders"]).round(2)
    kpis_by_product["revenue"] = kpis_by_product["revenue"].round(2)

    return kpis_global, kpis_by_country, kpis_by_product


@task(name="aggregate_time")
def aggregate_temporal(fact_achats: pd.DataFrame) -> Tuple[pd.DataFrame, pd.DataFrame, pd.DataFrame]:
    """Pré-calcule des agrégations temporelles : jour / semaine / mois."""

    df = fact_achats.copy()
    df["date"] = pd.to_datetime(df["date_achat"], errors="coerce")

    # -------------------
    # Par jour
    # -------------------
    agg_day = (
        df.groupby(df["date"].dt.strftime("%Y-%m-%d"))
        .agg(revenue=("montant", "sum"), orders=("id_achat", "nunique"), customers=("id_client", "nunique"))
        .reset_index()
        .rename(columns={"date": "day"})
    )
    agg_day.rename(columns={agg_day.columns[0]: "day"}, inplace=True)
    agg_day["revenue"] = agg_day["revenue"].round(2)

    # -------------------
    # Par semaine (ISO week)
    # -------------------
    df["year"] = df["date"].dt.isocalendar().year.astype(int)
    df["week"] = df["date"].dt.isocalendar().week.astype(int)

    agg_week = (
        df.groupby(["year", "week"])
        .agg(revenue=("montant", "sum"), orders=("id_achat", "nunique"), customers=("id_client", "nunique"))
        .reset_index()
    )
    agg_week["revenue"] = agg_week["revenue"].round(2)

    # -------------------
    # Par mois
    # -------------------
    df["month"] = df["date"].dt.strftime("%Y-%m")
    agg_month = (
        df.groupby(["month"])
        .agg(revenue=("montant", "sum"), orders=("id_achat", "nunique"), customers=("id_client", "nunique"))
        .reset_index()
    )
    agg_month["revenue"] = agg_month["revenue"].round(2)

    return agg_day, agg_week, agg_month


@flow(name="gold_ingestion_flow")
def gold_ingestion_flow(
    silver_clients_clean_object: str = "clients_clean.csv",
    silver_achats_clean_object: str = "achats_clean.csv",
    silver_promo_clean_object: str = "promo_clean.csv",
    gold_prefix: str = "",
) -> dict:
    """Gold flow:
    - Read clean data from SILVER
    - Build dimensions + fact tables
    - Compute KPIs
    - Pre-aggregate time series
    - Write outputs to GOLD bucket

    Output layout (MinIO GOLD bucket):
    - {gold_prefix}dim_clients.csv
    - {gold_prefix}dim_produits.csv
    - {gold_prefix}dim_date.csv
    - {gold_prefix}fact_achats.csv
    - {gold_prefix}dim_promos.csv
    - {gold_prefix}fact_promos.csv
    - {gold_prefix}kpi_global.csv
    - {gold_prefix}kpi_by_country.csv
    - {gold_prefix}kpi_by_product.csv
    - {gold_prefix}promo_kpi_global.csv
    - {gold_prefix}promo_kpi_by_name.csv
    - {gold_prefix}promo_kpi_by_country.csv
    - {gold_prefix}agg_day.csv
    - {gold_prefix}agg_week.csv
    - {gold_prefix}agg_month.csv
    """

    # 0) Bucket GOLD
    ensure_bucket_exists(BUCKET_GOLD)

    # 1) Read from SILVER
    clients_clean = read_csv_from_silver(silver_clients_clean_object)
    achats_clean = read_csv_from_silver(silver_achats_clean_object)
    promos_clean = read_csv_from_silver(silver_promo_clean_object)

    # 2) Build star schema tables
    dim_clients, dim_produits, dim_date = build_dimensions(clients_clean, achats_clean)
    fact_achats = build_fact_achats(achats_clean, dim_clients, dim_produits)

    # Promo star schema
    dim_promos, fact_promos = build_promo_tables(promos_clean, dim_clients)

    # 3) KPIs
    kpi_global, kpi_by_country, kpi_by_product = compute_kpis(fact_achats)
    promo_kpi_global, promo_kpi_by_name, promo_kpi_by_country = compute_promo_kpis(fact_promos)

    # 4) Temporal aggregations
    agg_day, agg_week, agg_month = aggregate_temporal(fact_achats)

    # 5) Write to GOLD
    outputs = {
        "dim_clients": write_csv_to_gold(dim_clients, f"{gold_prefix}dim_clients.csv"),
        "dim_produits": write_csv_to_gold(dim_produits, f"{gold_prefix}dim_produits.csv"),
        "dim_date": write_csv_to_gold(dim_date, f"{gold_prefix}dim_date.csv"),
        "fact_achats": write_csv_to_gold(fact_achats, f"{gold_prefix}fact_achats.csv"),
        "dim_promos": write_csv_to_gold(dim_promos, f"{gold_prefix}dim_promos.csv"),
        "fact_promos": write_csv_to_gold(fact_promos, f"{gold_prefix}fact_promos.csv"),
        "kpi_global": write_csv_to_gold(kpi_global, f"{gold_prefix}kpi_global.csv"),
        "kpi_by_country": write_csv_to_gold(kpi_by_country, f"{gold_prefix}kpi_by_country.csv"),
        "kpi_by_product": write_csv_to_gold(kpi_by_product, f"{gold_prefix}kpi_by_product.csv"),
        "promo_kpi_global": write_csv_to_gold(promo_kpi_global, f"{gold_prefix}promo_kpi_global.csv"),
        "promo_kpi_by_name": write_csv_to_gold(promo_kpi_by_name, f"{gold_prefix}promo_kpi_by_name.csv"),
        "promo_kpi_by_country": write_csv_to_gold(promo_kpi_by_country, f"{gold_prefix}promo_kpi_by_country.csv"),
        "agg_day": write_csv_to_gold(agg_day, f"{gold_prefix}agg_day.csv"),
        "agg_week": write_csv_to_gold(agg_week, f"{gold_prefix}agg_week.csv"),
        "agg_month": write_csv_to_gold(agg_month, f"{gold_prefix}agg_month.csv"),
    }

    latest_pointer = write_latest_run_pointer(gold_prefix)

    report = {
        "outputs": {**outputs, "latest_run_pointer": latest_pointer},
        "meta": {
            "bucket_silver": BUCKET_SILVER,
            "bucket_gold": BUCKET_GOLD,
            "silver_clients_clean_object": silver_clients_clean_object,
            "silver_achats_clean_object": silver_achats_clean_object,
            "silver_promo_clean_object": silver_promo_clean_object,
            "run_at": datetime.now().isoformat(timespec="seconds"),
        },
        "stats": {
            "clients_clean_rows": int(len(dim_clients)),
            "achats_clean_rows": int(len(fact_achats)),
            "promos_clean_rows": int(len(fact_promos)),
        },
    }

    return report


if __name__ == "__main__":
    result = gold_ingestion_flow()
    print("Gold ingestion completed:")
    print(result)
