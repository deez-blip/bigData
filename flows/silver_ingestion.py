from io import BytesIO
from datetime import datetime, date
from typing import Dict, Tuple

import pandas as pd
from prefect import flow, task

from config import BUCKET_BRONZE, BUCKET_SILVER, get_minio_client

# Référentiels simples (Silver) : listes autorisées
countries = [
    "France",
    "Germany",
    "Spain",
    "Italy",
    "Belgium",
    "Netherland",
    "Switzerland",
    "Portugal",
]

produits = [
    "Laptop",
    "Smartphone",
    "Tablet",
    "Headphones",
    "Smartwatch",
    "Camera",
    "Printer",
    "Monitor",
]

promo_names = [
    "WELCOME10",
    "SUMMER15",
    "BLACKFRIDAY20",
    "FREESHIP",
    "VIP25",
    "FLASH5",
    "STUDENT12",
]


@task(name="minio_read_csv", retries=3, retry_delay_seconds=2)
def read_csv_from_bronze(object_name: str) -> pd.DataFrame:
    """Lit un CSV depuis le bucket BRONZE et le charge en DataFrame pandas."""
    client = get_minio_client()
    obj = client.get_object(BUCKET_BRONZE, object_name)
    data = obj.read()
    obj.close()
    obj.release_conn()
    return pd.read_csv(BytesIO(data))


@task(name="minio_write_csv", retries=3, retry_delay_seconds=2)
def write_csv_to_silver(df: pd.DataFrame, object_name: str) -> str:
    """Ecrit un DataFrame en CSV dans le bucket SILVER."""
    client = get_minio_client()
    
    if not client.bucket_exists(BUCKET_SILVER):
        client.make_bucket(BUCKET_SILVER)
        
    buffer = BytesIO() # Create in-memory buffer
    df.to_csv(buffer, index=False) 
    buffer.seek(0)

    client.put_object(
        BUCKET_SILVER,
        object_name,
        buffer,
        length=buffer.getbuffer().nbytes,
        content_type="text/csv",
    )

    return object_name


@task(name="clean_clients")
def clean_clients_df(df: pd.DataFrame) -> Tuple[pd.DataFrame, pd.DataFrame, Dict[str, int]]:
    """Nettoie/valide la table clients et renvoie (clean, rejected, report)."""

    report: Dict[str, int] = {}

    # Copie de travail
    clients_df = df.copy()

    # Normalisations simples
    for col in ["nom", "email", "pays"]:
        if col in clients_df.columns:
            clients_df[col] = clients_df[col].astype("string").fillna("")
            clients_df[col] = clients_df[col].str.strip()

    # Standardisation date -> datetime
    clients_df["date_inscription"] = pd.to_datetime(
        clients_df.get("date_inscription"), errors="coerce", format="%Y-%m-%d"
    )

    today = pd.Timestamp(date.today())

    # Règles de validité (Silver)
    mask_id_ok = clients_df["id_client"].notna()
    mask_nom_ok = clients_df["nom"].astype(str).str.len() > 0
    mask_email_ok = clients_df["email"].astype(str).str.contains(r"^[^@\s]+@[^@\s]+\.[^@\s]+$", regex=True)
    mask_date_ok = clients_df["date_inscription"].notna() & (clients_df["date_inscription"] <= today)
    mask_country_ok = clients_df["pays"].isin(countries)

    valid_mask = mask_id_ok & mask_nom_ok & mask_email_ok & mask_date_ok & mask_country_ok

    rejected = clients_df.loc[~valid_mask].copy()
    clean = clients_df.loc[valid_mask].copy()

    # Cast types
    clean["id_client"] = pd.to_numeric(clean["id_client"], errors="coerce").astype("Int64")

    # Deduplication : 1 id_client = 1 ligne
    before_dedup = len(clean)
    clean = clean.drop_duplicates(subset=["id_client"], keep="first")
    report["clients_deduplicated"] = before_dedup - len(clean)

    # Remet la date au format ISO
    clean["date_inscription"] = clean["date_inscription"].dt.strftime("%Y-%m-%d")

    report["clients_total"] = len(clients_df)
    report["clients_clean"] = len(clean)
    report["clients_rejected"] = len(rejected)

    # Statistiques utiles pour debug
    report["clients_rejected_missing_id"] = int((~mask_id_ok).sum())
    report["clients_rejected_missing_nom"] = int((~mask_nom_ok).sum())
    report["clients_rejected_bad_email"] = int((~mask_email_ok).sum())
    report["clients_rejected_bad_date"] = int((~mask_date_ok).sum())
    report["clients_rejected_bad_country"] = int((~mask_country_ok).sum())

    return clean, rejected, report


@task(name="clean_achats")
def clean_achats_df(
    df: pd.DataFrame, clients_clean: pd.DataFrame
) -> Tuple[pd.DataFrame, pd.DataFrame, Dict[str, int]]:
    """Nettoie/valide la table achats et renvoie (clean, rejected, report)."""

    report: Dict[str, int] = {}

    achats_df = df.copy()

    # Normalisations
    if "produit" in achats_df.columns:
        achats_df["produit"] = achats_df["produit"].astype("string").fillna("")
        achats_df["produit"] = achats_df["produit"].str.strip()

    # Dates
    achats_df["date_achat"] = pd.to_datetime(
        achats_df.get("date_achat"), errors="coerce", format="%Y-%m-%d"
    )

    # Numériques
    achats_df["montant"] = pd.to_numeric(achats_df.get("montant"), errors="coerce")

    today = pd.Timestamp(date.today())

    # Règles Silver
    mask_id_achat_ok = achats_df["id_achat"].notna()
    mask_id_client_ok = achats_df["id_client"].notna()
    mask_date_ok = achats_df["date_achat"].notna() & (achats_df["date_achat"] <= today)
    mask_montant_ok = achats_df["montant"].notna() & (achats_df["montant"] > 0) & (achats_df["montant"] <= 5000)
    mask_produit_ok = achats_df["produit"].isin(produits)

    # Intégrité référentielle : id_client doit exister dans clients_clean
    clients_ids = set(pd.to_numeric(clients_clean["id_client"], errors="coerce").dropna().astype(int).tolist())
    mask_fk_ok = pd.to_numeric(achats_df["id_client"], errors="coerce").fillna(-999999).astype(int).isin(clients_ids)

    valid_mask = (
        mask_id_achat_ok
        & mask_id_client_ok
        & mask_date_ok
        & mask_montant_ok
        & mask_produit_ok
        & mask_fk_ok
    )

    rejected = achats_df.loc[~valid_mask].copy()
    clean = achats_df.loc[valid_mask].copy()

    # Cast types
    clean["id_achat"] = pd.to_numeric(clean["id_achat"], errors="coerce").astype("Int64")
    clean["id_client"] = pd.to_numeric(clean["id_client"], errors="coerce").astype("Int64")

    # Déduplication
    # 1) d'abord doublons exacts
    before_exact = len(clean)
    clean = clean.drop_duplicates(keep="first")
    report["achats_deduplicated_exact"] = before_exact - len(clean)

    # 2) ensuite unicité de id_achat si tu veux être strict (souvent clé primaire)
    before_id = len(clean)
    clean = clean.drop_duplicates(subset=["id_achat"], keep="first")
    report["achats_deduplicated_by_id"] = before_id - len(clean)

    # Remet date format ISO
    clean["date_achat"] = clean["date_achat"].dt.strftime("%Y-%m-%d")

    report["achats_total"] = len(achats_df)
    report["achats_clean"] = len(clean)
    report["achats_rejected"] = len(rejected)

    # Statistiques debug
    report["achats_rejected_missing_id_achat"] = int((~mask_id_achat_ok).sum())
    report["achats_rejected_missing_id_client"] = int((~mask_id_client_ok).sum())
    report["achats_rejected_bad_date"] = int((~mask_date_ok).sum())
    report["achats_rejected_bad_montant"] = int((~mask_montant_ok).sum())
    report["achats_rejected_bad_produit"] = int((~mask_produit_ok).sum())
    report["achats_rejected_fk"] = int((~mask_fk_ok).sum())

    return clean, rejected, report


@task(name="clean_promos")
def clean_promos_df(
    df: pd.DataFrame, clients_clean: pd.DataFrame
) -> Tuple[pd.DataFrame, pd.DataFrame, Dict[str, int]]:
    """Nettoie/valide la table promos et renvoie (clean, rejected, report)."""

    report: Dict[str, int] = {}

    promos_df = df.copy()

    # Normalisations
    if "promo_name" in promos_df.columns:
        promos_df["promo_name"] = promos_df["promo_name"].astype("string").fillna("")
        promos_df["promo_name"] = promos_df["promo_name"].str.strip()

    # Numériques
    promos_df["promo_value"] = pd.to_numeric(promos_df.get("promo_value"), errors="coerce")

    # Règles Silver
    mask_client_ok = promos_df["id_client"].notna()
    mask_promo_id_ok = promos_df["promo_id"].notna()
    mask_name_ok = promos_df["promo_name"].astype(str).str.len() > 0
    mask_name_ref_ok = promos_df["promo_name"].isin(promo_names)

    # promo_value : doit être > 0 et raisonnable
    mask_value_ok = promos_df["promo_value"].notna() & (promos_df["promo_value"] > 0) & (promos_df["promo_value"] <= 100)

    # Intégrité référentielle : id_client doit exister dans clients_clean
    clients_ids = set(pd.to_numeric(clients_clean["id_client"], errors="coerce").dropna().astype(int).tolist())
    mask_fk_ok = pd.to_numeric(promos_df["id_client"], errors="coerce").fillna(-999999).astype(int).isin(clients_ids)

    valid_mask = mask_client_ok & mask_promo_id_ok & mask_name_ok & mask_name_ref_ok & mask_value_ok & mask_fk_ok

    rejected = promos_df.loc[~valid_mask].copy()
    clean = promos_df.loc[valid_mask].copy()

    # Cast types
    clean["id_client"] = pd.to_numeric(clean["id_client"], errors="coerce").astype("Int64")
    clean["promo_id"] = pd.to_numeric(clean["promo_id"], errors="coerce").astype("Int64")

    # Déduplication
    # 1) doublons exacts
    before_exact = len(clean)
    clean = clean.drop_duplicates(keep="first")
    report["promos_deduplicated_exact"] = before_exact - len(clean)

    # 2) unicité promo_id (clé)
    before_id = len(clean)
    clean = clean.drop_duplicates(subset=["promo_id"], keep="first")
    report["promos_deduplicated_by_id"] = before_id - len(clean)

    report["promos_total"] = len(promos_df)
    report["promos_clean"] = len(clean)
    report["promos_rejected"] = len(rejected)

    # Statistiques debug
    report["promos_rejected_missing_id_client"] = int((~mask_client_ok).sum())
    report["promos_rejected_missing_promo_id"] = int((~mask_promo_id_ok).sum())
    report["promos_rejected_missing_name"] = int((~mask_name_ok).sum())
    report["promos_rejected_bad_name"] = int((~mask_name_ref_ok).sum())
    report["promos_rejected_bad_value"] = int((~mask_value_ok).sum())
    report["promos_rejected_fk"] = int((~mask_fk_ok).sum())

    return clean, rejected, report


@flow(name="silver_ingestion_flow")
def silver_ingestion_flow(
    bronze_clients_object: str = "clients.csv",
    bronze_achats_object: str = "achats.csv",
    bronze_promo_object: str = "promo.csv",
    silver_prefix: str = "",
) -> dict:
    """ 
    Silver flow:
    - Read data from BRONZE
    - Clean/validate/standardize
    - Write to SILVER

    Output layout (MinIO SILVER bucket):
    - {silver_prefix}clients_clean.csv
    - {silver_prefix}clients_rejected.csv
    - {silver_prefix}achats_clean.csv
    - {silver_prefix}achats_rejected.csv
    - {silver_prefix}promo_clean.csv
    - {silver_prefix}promo_rejected.csv
    """

    # 1) Read from Bronze
    clients_bronze = read_csv_from_bronze(bronze_clients_object)
    achats_bronze = read_csv_from_bronze(bronze_achats_object)
    promos_bronze = read_csv_from_bronze(bronze_promo_object)
    
    # 2) Clean
    clients_clean, clients_rejected, clients_report = clean_clients_df(clients_bronze)
    achats_clean, achats_rejected, achats_report = clean_achats_df(achats_bronze, clients_clean)
    promos_clean, promos_rejected, promos_report = clean_promos_df(promos_bronze, clients_clean)

    # 3) Write clean + rejected to Silver
    c_clean_name = write_csv_to_silver(clients_clean, f"{silver_prefix}clients_clean.csv")
    c_rej_name = write_csv_to_silver(clients_rejected, f"{silver_prefix}clients_rejected.csv")

    a_clean_name = write_csv_to_silver(achats_clean, f"{silver_prefix}achats_clean.csv")
    a_rej_name = write_csv_to_silver(achats_rejected, f"{silver_prefix}achats_rejected.csv")

    p_clean_name = write_csv_to_silver(promos_clean, f"{silver_prefix}promo_clean.csv")
    p_rej_name = write_csv_to_silver(promos_rejected, f"{silver_prefix}promo_rejected.csv")

    # 4) Report
    report = {
        "outputs": {
            "clients_clean": c_clean_name,
            "clients_rejected": c_rej_name,
            "achats_clean": a_clean_name,
            "achats_rejected": a_rej_name,
            "promo_clean": p_clean_name,
            "promo_rejected": p_rej_name,
        },
        "quality": {
            **clients_report,
            **achats_report,
            **promos_report,
        },
        "meta": {
            "bronze_clients_object": bronze_clients_object,
            "bronze_achats_object": bronze_achats_object,
            "bronze_promo_object": bronze_promo_object,
            "bucket_bronze": BUCKET_BRONZE,
            "bucket_silver": BUCKET_SILVER,
            "run_at": datetime.now().isoformat(timespec="seconds"),
        },
    }

    return report


if __name__ == "__main__":
    result = silver_ingestion_flow()
    print("Silver ingestion completed:")
    print(result)