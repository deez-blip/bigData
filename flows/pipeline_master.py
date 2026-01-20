import os
import time
from pathlib import Path
from datetime import datetime

from prefect import flow

from bronze_ingestion import bronze_ingestion_flow
from silver_ingestion import silver_ingestion_flow
from gold_ingestion import gold_ingestion_flow


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
        # 1) BRONZE (Pandas)
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
        # 2) SILVER (Pandas)
        # ------------------------------
        t_silver_start = time.perf_counter()
        silver_result = silver_ingestion_flow(
            bronze_clients_object=bronze_objects.get("clients", f"{bronze_prefix}clients.csv"),
            bronze_achats_object=bronze_objects.get("achats", f"{bronze_prefix}achats.csv"),
            bronze_promo_object=bronze_objects.get("promo", f"{bronze_prefix}promo.csv"),
            silver_prefix=silver_prefix,
        )
        silver_sec = time.perf_counter() - t_silver_start

        # ------------------------------
        # 3) GOLD (Pandas)
        # ------------------------------
        t_gold_start = time.perf_counter()
        gold_result = gold_ingestion_flow(
            silver_clients_clean_object=f"{silver_prefix}clients_clean.csv",
            silver_achats_clean_object=f"{silver_prefix}achats_clean.csv",
            silver_promo_clean_object=f"{silver_prefix}promo_clean.csv",
            gold_prefix=gold_prefix,
        )
        gold_sec = time.perf_counter() - t_gold_start

        total_sec = time.perf_counter() - t_total_start

        timings = {
            "engine": "pandas",
            "bronze_sec": round(bronze_sec, 4),
            "silver_sec": round(silver_sec, 4),
            "gold_sec": round(gold_sec, 4),
            "total_sec": round(total_sec, 4),
        }

        print("\n⏱️  Timings (Pandas)")
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