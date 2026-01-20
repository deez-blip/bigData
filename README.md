# Big Data Project — Pipeline ELT (Pandas & Spark)

Ce dépôt contient deux projets réalisés dans le cadre du cours de Big Data :

**1) Amélioration de la pipeline** *(sans Machine Learning)*  
**2) Big Data avec Spark** *(Spark cluster via Docker + transformations Spark)*

---

## Prérequis

Avant de lancer le projet, assure-toi d’avoir :

- Python + environnement virtuel (`.venv`)
- Docker + Docker Compose
- Prefect installé (pour l’orchestration)
- Streamlit installé (dashboard)

---

## Lancer Prefect

Après être entré dans ton environnement Python :

```bash
prefect server start
```

➡️ L’interface Prefect sera accessible sur :  
`http://localhost:4200`

---

## Pipeline Pandas (Bronze / Silver / Gold)

📌 **Objectif : tester la pipeline complète avec Pandas**

### Étapes :

**Terminal 1 : lancer le watcher Pandas**
```bash
python scripts/watch_sources.py
```

**Terminal 2 : générer les données**
```bash
python scripts/generate_data.py
```

📌 La génération actuelle produit environ **1 million de lignes** dans chaque CSV.  
➡️ Le watcher détecte l’ajout/modification des fichiers `.csv` dans `data/sources` et déclenche automatiquement :

Bronze → Silver → Gold

---

## Pipeline Spark (Big Data)

📌 **Objectif : remplacer Pandas par PySpark sur Silver et Gold**

### Étapes :

**Terminal 1 : lancer le watcher Spark**
```bash
python scripts/watch_sources_spark.py
```

**Terminal 2 : générer les données**
```bash
python scripts/generate_data.py
```

➡️ Le watcher détecte les changements et déclenche automatiquement :

Bronze → Silver Spark → Gold Spark

---

## Benchmark Pandas vs Spark (Comparatif performance)

📌 **Objectif : comparer le temps de traitement entre Pandas et Spark**

Assure-toi d’avoir déjà des fichiers dans `data/sources/` (clients / achats / promo)  
Puis lance :

```bash
python scripts/benchmark_pandas_vs_spark.py
```

➡️ Le script exécute les 2 pipelines et affiche le temps de traitement :

- Bronze
- Silver
- Gold
- Total

Un fichier CSV de résultats est aussi généré :

📄 `data/benchmark_results.csv`

---

## Dashboard Streamlit

Le dashboard Streamlit affiche automatiquement les résultats du **dernier run Gold**.

Il se base sur le fichier pointeur :

📌 `gold/latest_run.json`

➡️ Ce fichier contient le `run_id` et le `gold_prefix` à lire, ce qui permet de garder un historique des runs sans écraser les précédents.

---

## Résumé des scripts importants

| Script | Rôle |
|-------|------|
| `scripts/generate_data.py` | Génère des CSV (≈ 1M lignes) avec quelques erreurs volontaires |
| `scripts/watch_sources.py` | Watcher auto-run pipeline Pandas |
| `scripts/watch_sources_spark.py` | Watcher auto-run pipeline Spark |
| `scripts/benchmark_pandas_vs_spark.py` | Comparatif temps Pandas vs Spark |
| `dashboard/streamlit_app.py` | Dashboard Streamlit connecté à MinIO |

---

## Notes

- Le projet utilise un système de **run_id** pour historiser les runs.
- Les layers Bronze / Silver / Gold sont stockées dans MinIO.
- Le système est conçu pour être **automatique** : ajout/modification de CSV → pipeline relancée.

---
