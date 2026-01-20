import os
import subprocess
import time
from pathlib import Path
from datetime import datetime

from prefect import flow

from bronze_ingestion import bronze_ingestion_flow


# Lock global (empêche 2 exécutions en parallèle)
PROJECT_ROOT = Path(__file__).resolve().parent.parent
LOCK_PATH = PROJECT_ROOT / ".pipeline.lock"


def acquire_lock() -> bool:
    """Crée un lock file de manière atomique. Renvoie True si lock obtenu."""
    try:
        fd = os.open(str(LOCK_PATH), os.O_CREAT | os.O_EXCL | os.O_WRONLY)
        with os.fdopen(fd, "w") as f:
            f.write(str(os.getpid()))
        return True
    except FileExistsError:
        return False


def release_lock() -> None:
    """Supprime le lock file si présent."""
    try:
        LOCK_PATH.unlink(missing_ok=True)
    except Exception:
        pass

def write_latest_run_pointer_host(run_id: str) -> None:
    """Écrit gold/latest_run.json comme UN SEUL fichier JSON (host side).

    - Supprime l'ancien dossier Spark `gold/latest_run.json/` si présent (part-*.json)
    - Écrase le fichier `gold/latest_run.json`

    Cela évite d'avoir plusieurs `part-*.json` et casse Streamlit.
    """
    try:
        import json
        from io import BytesIO
        from minio import Minio

        endpoint = os.getenv("MINIO_ENDPOINT", "localhost:9000")
        access_key = os.getenv("MINIO_ACCESS_KEY", "minioadmin")
        secret_key = os.getenv("MINIO_SECRET_KEY", "minioadmin")

        client = Minio(endpoint, access_key=access_key, secret_key=secret_key, secure=False)

        # 1) Cleanup Spark folder (if any)
        for obj in client.list_objects("gold", prefix="latest_run.json/", recursive=True):
            client.remove_object("gold", obj.object_name)

        payload = {
            "run_id": run_id,
            "gold_prefix": f"runs/{run_id}/",
            "generated_at": datetime.now().isoformat(timespec="seconds"),
        }

        data = json.dumps(payload, ensure_ascii=False, indent=2).encode("utf-8")
        client.put_object(
            "gold",
            "latest_run.json",
            BytesIO(data),
            length=len(data),
            content_type="application/json",
        )

        print("✅ latest_run.json written as single object (host)")

    except Exception as e:
        print("⚠️ Unable to write latest_run.json from host:", e)

@flow(name="pipeline_master_flow")
def pipeline_master_flow() -> dict:
    # Empêche les doubles runs
    if not acquire_lock():
        print("⚠️  Pipeline déjà en cours d'exécution. Run ignoré.")
        return {"status": "skipped", "reason": "already_running"}

    # Identifiant unique de run (historisation)
    run_id = datetime.now().strftime("%Y%m%d_%H%M%S")
    bronze_prefix = f"runs/{run_id}/"
    silver_prefix = f"runs/{run_id}/"
    gold_prefix = f"runs/{run_id}/"

    try:
        t_total_start = time.perf_counter()

        # ------------------------------
        # 1) BRONZE (python)
        # ------------------------------
        t_bronze_start = time.perf_counter()
        bronze_result = bronze_ingestion_flow(
            data_dir="./data/sources",
            run_id=run_id,
            bronze_prefix=bronze_prefix,
            sources_prefix=bronze_prefix,
        )
        bronze_sec = time.perf_counter() - t_bronze_start

        # Bronze renvoie les chemins des fichiers écrits dans le bucket bronze
        bronze_objects = bronze_result.get("bronze_objects", {})

        # ------------------------------
        # 2) SILVER (Spark)
        # ------------------------------
        spark_packages = "org.apache.hadoop:hadoop-aws:3.3.4"

        # Copie des scripts Spark dans le container (évite les problèmes de volume /app)
        local_silver_script = str((PROJECT_ROOT / "flows" / "silver_ingestion_spark.py").resolve())
        local_gold_script = str((PROJECT_ROOT / "flows" / "gold_ingestion_spark.py").resolve())

        cp_silver = ["docker", "cp", local_silver_script, "spark-master:/tmp/silver_ingestion_spark.py"]
        cp_gold = ["docker", "cp", local_gold_script, "spark-master:/tmp/gold_ingestion_spark.py"]

        print("\n📦 Copy Spark scripts into spark-master:")
        print(" ".join(cp_silver))
        subprocess.run(cp_silver, check=False)
        print(" ".join(cp_gold))
        subprocess.run(cp_gold, check=False)

        silver_cmd = [
            "docker",
            "exec",
            "spark-master",
            "spark-submit",
            "--master",
            "spark://spark-master:7077",
            "--packages",
            spark_packages,
            "/tmp/silver_ingestion_spark.py",
            "--run-id",
            run_id,
        ]

        t_silver_start = time.perf_counter()
        print("\n🚀 Running Silver Spark job:")
        print(" ".join(silver_cmd))
        silver_proc = subprocess.run(silver_cmd, check=False)
        silver_sec = time.perf_counter() - t_silver_start

        silver_result = {
            "status": "ok" if silver_proc.returncode == 0 else "failed",
            "returncode": silver_proc.returncode,
            "silver_prefix": silver_prefix,
            "bronze_objects": bronze_objects,
        }

        # ------------------------------
        # 3) GOLD (Spark)
        # ------------------------------
        gold_cmd = [
            "docker",
            "exec",
            "spark-master",
            "spark-submit",
            "--master",
            "spark://spark-master:7077",
            "--packages",
            spark_packages,
            "/tmp/gold_ingestion_spark.py",
            "--run-id",
            run_id,
        ]

        t_gold_start = time.perf_counter()
        print("\n🚀 Running Gold Spark job:")
        print(" ".join(gold_cmd))
        gold_proc = subprocess.run(gold_cmd, check=False)
        write_latest_run_pointer_host(run_id)
        gold_sec = time.perf_counter() - t_gold_start

        gold_result = {
            "status": "ok" if gold_proc.returncode == 0 else "failed",
            "returncode": gold_proc.returncode,
            "gold_prefix": gold_prefix,
        }

        total_sec = time.perf_counter() - t_total_start

        timings = {
            "engine": "spark",
            "bronze_sec": round(bronze_sec, 4),
            "silver_sec": round(silver_sec, 4),
            "gold_sec": round(gold_sec, 4),
            "total_sec": round(total_sec, 4),
        }

        print("\n⏱️  Timings (Spark)")
        print(timings)

        return {
            "run_id": run_id,
            "prefixes": {
                "bronze": bronze_prefix,
                "silver": silver_prefix,
                "gold": gold_prefix,
            },
            "bronze": bronze_result,
            "silver": silver_result,
            "gold": gold_result,
            "timings": timings,
        }

    finally:
        release_lock()


if __name__ == "__main__":
    result = pipeline_master_flow()
    print("Pipeline master completed:")
    print(result)