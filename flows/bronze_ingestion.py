from io import BytesIO
from pathlib import Path
from datetime import datetime

from prefect import flow, task

from config import BUCKET_BRONZE, BUCKET_SOURCES, get_minio_client

@task(name="upload_csv_to_sources", retries=3)
def upload_csv_to_sources(file_path: str, object_name: str) -> str:
    """
    Upload local CSV file to the sources bucket in MinIO
    
    Args:
        file_path (str): Path to the CSV file
        object_name (str): Name of the object in the MinIO bucket
        
    Returns:
        Objec name in MinIO
    """
    
    client = get_minio_client()
    
    if not client.bucket_exists(BUCKET_SOURCES):
        client.make_bucket(BUCKET_SOURCES)
        
    client.fput_object(
        BUCKET_SOURCES,
        object_name,
        file_path)
    
    print(f"Uploaded {file_path} to bucket {BUCKET_SOURCES} as {object_name}")
    
    return object_name

@task(name="copy_to_bronze_layer", retries=3)
def copy_to_bronze_layer(source_object_name: str, dest_object_name: str | None = None) -> str:
    """
    Copy object from sources bucket to bronze bucket in MinIO
    
    Args:
        source_object_name (str): Name of the object in the sources bucket
        dest_object_name (str | None): Name of the object in the bronze bucket (defaults to same as source)

    Returns:
        Object name in bronze bucket
    """
    
    client = get_minio_client()
    
    if not client.bucket_exists(BUCKET_BRONZE):
        client.make_bucket(BUCKET_BRONZE)
    
    if dest_object_name is None:
        dest_object_name = source_object_name
    
    response = client.get_object(BUCKET_SOURCES, source_object_name)
    data = response.read()
    response.close()
    response.release_conn()
    
    client.put_object(
        BUCKET_BRONZE,
        dest_object_name,
        BytesIO(data),
        length=len(data)
    )
    
    print(f"Copied {source_object_name} from {BUCKET_SOURCES} to {BUCKET_BRONZE} as {dest_object_name}")
    
    return dest_object_name

@flow(name="bronze_ingestion_flow")
def bronze_ingestion_flow(
    data_dir: str = "./data/sources",
    run_id: str | None = None,
    bronze_prefix: str = "",
    sources_prefix: str = "",
) -> dict:
    """
    Flow to ingest data from local CSV files to MinIO bronze layer
    
    Args:
        data_dir (str): Directory containing source CSV files
    Returns:
        dict: Mapping of object names in bronze bucket
    """
    
    data_path = Path(data_dir)
    
    # Si aucun run_id fourni, on en génère un (utile si on lance Bronze seul)
    if run_id is None:
        run_id = datetime.now().strftime("%Y%m%d_%H%M%S")

    # Normalisation des prefixes (ex: "runs/<run_id>/")
    if bronze_prefix and not bronze_prefix.endswith("/"):
        bronze_prefix += "/"
    if sources_prefix and not sources_prefix.endswith("/"):
        sources_prefix += "/"
    
    csv_files = list(data_path.glob("*.csv"))

    bronze_objects: dict[str, str] = {}

    for csv_path in csv_files:
        # Ex: "clients.csv"
        base_name = csv_path.name

        # Ex: "runs/<run_id>/clients.csv"
        sources_object_name = f"{sources_prefix}{base_name}" if sources_prefix else base_name
        bronze_object_name = f"{bronze_prefix}{base_name}" if bronze_prefix else base_name

        upload_csv_to_sources(str(csv_path), sources_object_name)
        dest_name = copy_to_bronze_layer(sources_object_name, bronze_object_name)

        # Mapping pratique (clients/achats/promo)
        dataset_key = base_name.replace(".csv", "")
        bronze_objects[dataset_key] = dest_name
    
    return {
        "status": "Bronze ingestion completed",
        "run_id": run_id,
        "bronze_prefix": bronze_prefix,
        "sources_prefix": sources_prefix,
        "bronze_objects": bronze_objects,
    }
    
if __name__ == "__main__":
    result = bronze_ingestion_flow()
    print("Bronze ingestion completed:", result)