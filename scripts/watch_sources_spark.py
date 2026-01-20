import time
import subprocess
import threading
from pathlib import Path

from watchdog.observers import Observer
from watchdog.events import FileSystemEventHandler


WATCH_DIR = "./data/sources"
PROJECT_ROOT = Path(__file__).resolve().parent.parent
LOCK_PATH = PROJECT_ROOT / ".pipeline.lock"


class Handler(FileSystemEventHandler):
    def __init__(self, debounce_seconds: float = 2.0):
        self.debounce_seconds = debounce_seconds
        self._timer: threading.Timer | None = None
        self._changed_files: set[str] = set()
        self._lock = threading.Lock()

    def _schedule_run(self):
        # Annule le timer précédent si un autre événement arrive avant la fin du délai
        if self._timer is not None:
            self._timer.cancel()

        self._timer = threading.Timer(self.debounce_seconds, self._run_pipeline_once)
        self._timer.daemon = True
        self._timer.start()

    def _run_pipeline_once(self):
        with self._lock:
            changed = sorted(self._changed_files)
            self._changed_files.clear()

        if not changed:
            return

        print("\nChangements détectés (batch) :")
        for f in changed:
            print(" -", f)
        print("Lancement unique de la pipeline complète...")

        # Si la pipeline est déjà en cours, on ne relance pas une 2ème fois
        if LOCK_PATH.exists():
            print("⏳ Pipeline déjà en cours (lock détecté). Nouveau run ignoré.")
            return

        proc = subprocess.run(["python", "flows/pipeline_master_spark.py"], check=False)
        if proc.returncode != 0:
            print(f"❌ pipeline_master_spark failed with return code {proc.returncode}")

    def on_any_event(self, event):
        # On ignore les dossiers
        if event.is_directory:
            return

        # On ne déclenche que sur les CSV
        src_path = getattr(event, "src_path", "") or ""
        if not src_path.endswith(".csv"):
            return

        # Normalise le chemin (utile sur macOS)
        p = Path(src_path).resolve()

        with self._lock:
            self._changed_files.add(str(p))

        # Debounce : on regroupe les changements et on ne lance qu'une seule fois
        self._schedule_run()


if __name__ == "__main__":
    observer = Observer()
    observer.schedule(Handler(debounce_seconds=2.0), WATCH_DIR, recursive=False)
    observer.start()
    print(f"Watcher actif sur {WATCH_DIR}")

    try:
        while True:
            time.sleep(1)
    except KeyboardInterrupt:
        observer.stop()
    observer.join()