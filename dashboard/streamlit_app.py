from io import BytesIO
import json
import pandas as pd
import streamlit as st

from config import get_minio_client

# Buckets
BUCKET_GOLD = "gold"  # (ou import BUCKET_GOLD si tu l'as ajouté dans config.py)

# Fichiers Gold
FILES = {
    "KPI Global": "kpi_global.csv",
    "KPI by Country": "kpi_by_country.csv",
    "KPI by Product": "kpi_by_product.csv",
    "Agg Day": "agg_day.csv",
    "Agg Week": "agg_week.csv",
    "Agg Month": "agg_month.csv",
    "Fact Achats": "fact_achats.csv",
    "Dim Clients": "dim_clients.csv",
    "Dim Produits": "dim_produits.csv",
    "Dim Date": "dim_date.csv",
}

def resolve_csv_object_key(bucket: str, object_key: str) -> str:
    """Résout une "clé" CSV qui peut être un dossier Spark.

    Spark écrit ses CSV comme un dossier contenant des fichiers `part-*.csv`.
    Exemple :
      gold/runs/<run_id>/kpi_by_country.csv/part-0000....csv

    Cette fonction retourne une clé MinIO lisible (fichier CSV réel).
    """
    client = get_minio_client()

    # 1) Essai direct : fichier classique (1 seul CSV)
    try:
        client.stat_object(bucket, object_key)
        return object_key
    except Exception:
        pass

    # 2) Essai "dossier Spark" : on cherche un part-*.csv
    prefix = object_key.rstrip("/") + "/"

    objects = list(client.list_objects(bucket, prefix=prefix, recursive=True))
    part_files = [
        o.object_name
        for o in objects
        if o.object_name.startswith(prefix + "part-") and o.object_name.endswith(".csv")
    ]

    if part_files:
        part_files.sort()
        return part_files[0]

    raise FileNotFoundError(
        f"Aucun CSV lisible trouvé pour '{object_key}' (ni dans le dossier Spark '{prefix}')"
    )

def resolve_json_object_key(bucket: str, object_key: str) -> str:
    """Résout une "clé" JSON qui peut être un dossier Spark.

    Spark écrit ses JSON comme un dossier contenant des fichiers `part-*.json`.
    Exemple :
      gold/latest_run.json/part-0000....json

    Cette fonction retourne une clé MinIO lisible (fichier JSON réel).
    """
    client = get_minio_client()

    # 1) Essai direct : fichier classique (1 seul JSON)
    try:
        client.stat_object(bucket, object_key)
        return object_key
    except Exception:
        pass

    # 2) Essai "dossier Spark" : on cherche un part-*.json
    prefix = object_key.rstrip("/") + "/"

    objects = list(client.list_objects(bucket, prefix=prefix, recursive=True))
    part_files = [
        o.object_name
        for o in objects
        if o.object_name.startswith(prefix + "part-") and o.object_name.endswith(".json")
    ]

    if part_files:
        part_files.sort()
        return part_files[0]

    raise FileNotFoundError(
        f"Aucun JSON lisible trouvé pour '{object_key}' (ni dans le dossier Spark '{prefix}')"
    )

@st.cache_data(show_spinner=False, ttl=5)
def read_csv_from_minio(bucket: str, object_name: str) -> pd.DataFrame:
    client = get_minio_client()

    # Résout automatiquement dossier Spark -> fichier part-*.csv
    resolved_key = resolve_csv_object_key(bucket, object_name)

    obj = client.get_object(bucket, resolved_key)
    data = obj.read()

    # on libère la connexion MinIO
    try:
        obj.close()
        obj.release_conn()
    except Exception:
        pass

    return pd.read_csv(BytesIO(data))


# --- Cached JSON reader from MinIO
@st.cache_data(show_spinner=False, ttl=5)
def read_json_from_minio(bucket: str, object_name: str) -> dict:
    client = get_minio_client()

    # Résout automatiquement dossier Spark -> fichier part-*.json
    resolved_key = resolve_json_object_key(bucket, object_name)

    obj = client.get_object(bucket, resolved_key)
    data = obj.read()

    try:
        obj.close()
        obj.release_conn()
    except Exception:
        pass

    return json.loads(data.decode("utf-8"))


st.set_page_config(page_title="Big Data Dashboard", layout="wide")

st.title("📊 Dashboard Big Data (Gold Layer)")

# ---- Latest run pointer (Gold) ----
latest_prefix = ""
latest_run_id = ""

try:
    # Affiche la clé réellement lue (utile si Spark écrit un dossier)
    try:
        resolved_latest_key = resolve_json_object_key(BUCKET_GOLD, "latest_run.json")
        st.caption(f"📌 latest_run.json lu depuis : `{resolved_latest_key}`")
    except Exception:
        pass

    pointer = read_json_from_minio(BUCKET_GOLD, "latest_run.json")
    latest_prefix = pointer.get("gold_prefix", "") or ""
    latest_run_id = pointer.get("run_id", "") or ""
except Exception:
    # Si le fichier n'existe pas encore, on fallback sur la racine du bucket
    latest_prefix = ""
    latest_run_id = ""

if latest_prefix and not latest_prefix.endswith("/"):
    latest_prefix += "/"

if latest_run_id:
    st.caption(f"✅ Dernier run détecté : `{latest_run_id}` (prefix: `{latest_prefix}`)")
else:
    st.caption("⚠️ Aucun latest_run.json détecté : lecture à la racine du bucket gold")

# Sidebar
st.sidebar.header("Navigation")
choice = st.sidebar.selectbox("Choisir une table / KPI", list(FILES.keys()))

base_file_name = FILES[choice]
file_name = f"{latest_prefix}{base_file_name}" if latest_prefix else base_file_name

# Preview de la clé réellement lue (si Spark a écrit un dossier)
try:
    resolved_preview = resolve_csv_object_key(BUCKET_GOLD, file_name)
except Exception:
    resolved_preview = file_name

st.subheader(f"📁 {choice} → `{base_file_name}`")
st.caption(f"Objet MinIO demandé : `{file_name}`")
st.caption(f"Objet MinIO lu : `{resolved_preview}`")

try:
    df = read_csv_from_minio(BUCKET_GOLD, file_name)
except Exception as e:
    st.error(f"Impossible de lire `{file_name}` dans le bucket `{BUCKET_GOLD}`")
    st.exception(e)
    st.stop()

# Display
st.dataframe(df, use_container_width=True)

# Bonus : affichage KPI global en cards
if choice == "KPI Global" and not df.empty:
    kpi = df.iloc[0]
    col1, col2, col3, col4 = st.columns(4)

    col1.metric("💰 Total Revenue", f"{kpi['total_revenue']:.2f} €")
    col2.metric("🧾 Total Orders", int(kpi["total_orders"]))
    col3.metric("👥 Unique Customers", int(kpi["unique_customers"]))
    col4.metric("🛒 Avg Order Value", f"{kpi['avg_order_value']:.2f} €")

# Bonus : graphique simple si agg_day / month
if choice in ["Agg Day", "Agg Month"] and not df.empty:
    st.divider()
    st.subheader("📈 Evolution du CA")

    # essaye de détecter la colonne temps
    time_col = "day" if "day" in df.columns else "month" if "month" in df.columns else None

    if time_col:
        df_plot = df[[time_col, "revenue"]].copy()
        df_plot = df_plot.sort_values(time_col)
        st.line_chart(df_plot.set_index(time_col))
    else:
        st.info("Aucune colonne temporelle détectée pour tracer un graphe.")