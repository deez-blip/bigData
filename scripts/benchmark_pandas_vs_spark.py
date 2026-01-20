import subprocess
import re
import ast
import pandas as pd


def run_pipeline(cmd: list[str], name: str) -> dict:
    """
    Lance un pipeline (Pandas ou Spark) via subprocess,
    récupère le dict 'timings' affiché dans la console.
    """
    print(f"\n===============================")
    print(f"🚀 Running {name}")
    print(f"CMD: {' '.join(cmd)}")
    print(f"===============================\n")

    result = subprocess.run(cmd, capture_output=True, text=True)

    # Affiche tout le log du pipeline (pratique pour debug)
    print(result.stdout)
    if result.stderr:
        print("⚠️ STDERR:\n", result.stderr)

    if result.returncode != 0:
        raise RuntimeError(f"{name} failed (returncode={result.returncode})")

    # Cherche le dernier dict timings affiché
    # ex: {'engine': 'spark', 'bronze_sec': 0.12, ...}
    timings = None
    for line in reversed(result.stdout.splitlines()):
        if line.strip().startswith("{'engine':"):
            timings = ast.literal_eval(line.strip())
            break

    if timings is None:
        raise ValueError(f"Impossible de trouver le dict timings dans la sortie de {name}")

    return timings


def main():
    # 1) Pandas pipeline
    pandas_timings = run_pipeline(
        ["python", "flows/pipeline_master.py"],
        name="Pandas Pipeline"
    )

    # 2) Spark pipeline
    spark_timings = run_pipeline(
        ["python", "flows/pipeline_master_spark.py"],
        name="Spark Pipeline"
    )

    # Comparaison
    df = pd.DataFrame([pandas_timings, spark_timings])
    df["total_vs_pandas_ratio"] = df["total_sec"] / df.loc[df["engine"] == "pandas", "total_sec"].values[0]

    print("\n✅ Résultat Benchmark (Pandas vs Spark)")
    print(df)

    # Sauvegarde local (optionnel)
    out_path = "data/benchmark_results.csv"
    df.to_csv(out_path, index=False)
    print(f"\n📁 Résultats sauvegardés dans : {out_path}")


if __name__ == "__main__":
    main()