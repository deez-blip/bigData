import csv
import random
from datetime import datetime, timedelta
from pathlib import Path

# IMPORTANT PERF NOTE
# For 1,000,000+ rows, avoid Faker for every row (too slow) and avoid keeping
# all rows in memory. We stream-write rows directly to CSV.

random.seed(42)


def _random_date_between(start_days_ago: int, end_days_ago: int) -> str:
    """Return a date string YYYY-MM-DD between now-start_days_ago and now-end_days_ago."""
    if start_days_ago < end_days_ago:
        start_days_ago, end_days_ago = end_days_ago, start_days_ago

    start = datetime.now() - timedelta(days=start_days_ago)
    end = datetime.now() - timedelta(days=end_days_ago)
    delta_days = (end - start).days
    if delta_days <= 0:
        return start.strftime("%Y-%m-%d")

    d = start + timedelta(days=random.randint(0, delta_days))
    return d.strftime("%Y-%m-%d")


def generate_clients(n_clients: int, output_path: str | Path) -> int:
    """Generate clients.csv with ~n_clients lines.

    Returns:
        int: max client id generated (useful for achats/promos random FK generation)
    """

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

    output_path = Path(output_path)
    output_path.parent.mkdir(parents=True, exist_ok=True)

    with open(output_path, "w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(
            f,
            fieldnames=["id_client", "nom", "email", "date_inscription", "pays"],
        )
        writer.writeheader()

        for i in range(1, n_clients + 1):
            date_inscription = _random_date_between(start_days_ago=3 * 365, end_days_ago=30)

            # Injecte parfois des lignes avec UNE seule erreur (rejet ciblé en Silver)
            # NB: modulos "premiers" pour limiter les overlaps.
            if i % 29 == 0:
                # 1 erreur : id_client manquant
                writer.writerow(
                    {
                        "id_client": "",
                        "nom": f"Client_{i}",
                        "email": f"client{i}@example.com",
                        "date_inscription": date_inscription,
                        "pays": random.choice(countries),
                    }
                )
                continue

            if i % 13 == 0:
                # 1 erreur : nom manquant
                writer.writerow(
                    {
                        "id_client": i,
                        "nom": "",
                        "email": f"client{i}@example.com",
                        "date_inscription": date_inscription,
                        "pays": random.choice(countries),
                    }
                )
                continue

            if i % 17 == 0:
                # 1 erreur : email invalide
                writer.writerow(
                    {
                        "id_client": i,
                        "nom": f"Client_{i}",
                        "email": "not-an-email",
                        "date_inscription": date_inscription,
                        "pays": random.choice(countries),
                    }
                )
                continue

            if i % 19 == 0:
                # 1 erreur : date_inscription dans le futur
                future_date = (datetime.now() + timedelta(days=365)).strftime("%Y-%m-%d")
                writer.writerow(
                    {
                        "id_client": i,
                        "nom": f"Client_{i}",
                        "email": f"client{i}@example.com",
                        "date_inscription": future_date,
                        "pays": random.choice(countries),
                    }
                )
                continue

            if i % 23 == 0:
                # 1 erreur : pays hors référentiel
                writer.writerow(
                    {
                        "id_client": i,
                        "nom": f"Client_{i}",
                        "email": f"client{i}@example.com",
                        "date_inscription": date_inscription,
                        "pays": "Narnia",
                    }
                )
                continue

            # Injecte volontairement quelques lignes avec PLUSIEURS erreurs
            if i % 10 == 0:
                writer.writerow(
                    {
                        "id_client": "",
                        "nom": "",
                        "email": "invalid_email_format",
                        "date_inscription": (datetime.now() + timedelta(days=365)).strftime("%Y-%m-%d"),
                        "pays": "Narnia",
                    }
                )
                continue

            # Doublons logiques : de temps en temps on duplique un id_client (devrait être unique)
            # On crée un doublon en réécrivant un ancien id (i - 1).
            if i % 50 == 0 and i > 1:
                dup_id = i - 1
                writer.writerow(
                    {
                        "id_client": dup_id,
                        "nom": f"Client_DUP_{dup_id}",
                        "email": f"client{dup_id}@example.com",
                        "date_inscription": date_inscription,
                        "pays": random.choice(countries),
                    }
                )
                continue

            # Ligne valide
            writer.writerow(
                {
                    "id_client": i,
                    "nom": f"Client_{i}",
                    "email": f"client{i}@example.com",
                    "date_inscription": date_inscription,
                    "pays": random.choice(countries),
                }
            )

    print(f"Generated {n_clients} clients and saved to {output_path}")
    return n_clients


def generate_achats(max_client_id: int, n_achats: int, output_path: str | Path) -> None:
    """Generate achats.csv with ~n_achats lines."""

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

    invalid_produits = ["Toaster", "DragonEgg", "", None]

    output_path = Path(output_path)
    output_path.parent.mkdir(parents=True, exist_ok=True)

    with open(output_path, "w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(
            f,
            fieldnames=["id_achat", "id_client", "date_achat", "montant", "produit"],
        )
        writer.writeheader()

        for achat_id in range(1, n_achats + 1):
            # Achat valide de base
            valid_client = random.randint(1, max_client_id)
            valid_date = _random_date_between(start_days_ago=365, end_days_ago=0)
            valid_montant = round(random.uniform(20.0, 2000.0), 2)
            valid_produit = random.choice(produits)

            # Injecte parfois des lignes avec UNE seule erreur
            if achat_id % 37 == 0:
                # 1 erreur : id_achat manquant
                writer.writerow(
                    {
                        "id_achat": "",
                        "id_client": valid_client,
                        "date_achat": valid_date,
                        "montant": valid_montant,
                        "produit": valid_produit,
                    }
                )
                continue

            if achat_id % 41 == 0:
                # 1 erreur : id_client orphelin
                writer.writerow(
                    {
                        "id_achat": achat_id,
                        "id_client": 999999,
                        "date_achat": valid_date,
                        "montant": valid_montant,
                        "produit": valid_produit,
                    }
                )
                continue

            if achat_id % 43 == 0:
                # 1 erreur : date_achat dans le futur
                bad_date = (datetime.now() + timedelta(days=random.randint(30, 365))).strftime("%Y-%m-%d")
                writer.writerow(
                    {
                        "id_achat": achat_id,
                        "id_client": valid_client,
                        "date_achat": bad_date,
                        "montant": valid_montant,
                        "produit": valid_produit,
                    }
                )
                continue

            if achat_id % 47 == 0:
                # 1 erreur : montant négatif
                writer.writerow(
                    {
                        "id_achat": achat_id,
                        "id_client": valid_client,
                        "date_achat": valid_date,
                        "montant": round(random.uniform(-500.0, -1.0), 2),
                        "produit": valid_produit,
                    }
                )
                continue

            if achat_id % 53 == 0:
                # 1 erreur : montant manquant
                writer.writerow(
                    {
                        "id_achat": achat_id,
                        "id_client": valid_client,
                        "date_achat": valid_date,
                        "montant": "",
                        "produit": valid_produit,
                    }
                )
                continue

            if achat_id % 59 == 0:
                # 1 erreur : produit invalide
                writer.writerow(
                    {
                        "id_achat": achat_id,
                        "id_client": valid_client,
                        "date_achat": valid_date,
                        "montant": valid_montant,
                        "produit": "DragonEgg",
                    }
                )
                continue

            # Injecte parfois des lignes avec PLUSIEURS erreurs
            if achat_id % 20 == 0:
                bad_date = (datetime.now() + timedelta(days=random.randint(30, 365))).strftime("%Y-%m-%d")
                bad_amount = random.choice(
                    [
                        round(random.uniform(-500.0, -1.0), 2),
                        round(random.uniform(10000.0, 50000.0), 2),
                        0.0,
                        "",
                    ]
                )
                bad_client = random.choice(["", -1, 999999])

                writer.writerow(
                    {
                        "id_achat": "",
                        "id_client": bad_client,
                        "date_achat": bad_date,
                        "montant": bad_amount,
                        "produit": random.choice(invalid_produits),
                    }
                )
                continue

            # Doublons exacts : duplique de temps en temps la ligne précédente
            if achat_id % 100 == 0 and achat_id > 1:
                # Un doublon exact (même id_achat) ne sera pas possible ici, donc on duplique la ligne
                # en gardant un id_achat identique volontairement (rejet/dedup)
                writer.writerow(
                    {
                        "id_achat": achat_id - 1,
                        "id_client": valid_client,
                        "date_achat": valid_date,
                        "montant": valid_montant,
                        "produit": valid_produit,
                    }
                )
                continue

            # Ligne valide
            writer.writerow(
                {
                    "id_achat": achat_id,
                    "id_client": valid_client,
                    "date_achat": valid_date,
                    "montant": valid_montant,
                    "produit": valid_produit,
                }
            )

    print(f"Generated {n_achats} achats and saved to {output_path}")


def generate_promos(max_client_id: int, n_promos: int, output_path: str | Path) -> None:
    """Generate promo.csv with ~n_promos lines."""

    promo_names = [
        "WELCOME10",
        "SUMMER15",
        "BLACKFRIDAY20",
        "FREESHIP",
        "VIP25",
        "FLASH5",
        "STUDENT12",
    ]

    output_path = Path(output_path)
    output_path.parent.mkdir(parents=True, exist_ok=True)

    with open(output_path, "w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(
            f,
            fieldnames=["id_client", "promo_id", "promo_name", "promo_value"],
        )
        writer.writeheader()

        for promo_id in range(1, n_promos + 1):
            valid_client = random.randint(1, max_client_id)
            valid_name = random.choice(promo_names)
            valid_value = round(random.uniform(1.0, 50.0), 2)

            # Injecte parfois des lignes avec UNE seule erreur
            if promo_id % 31 == 0:
                # 1 erreur : promo_id manquant
                writer.writerow(
                    {
                        "id_client": valid_client,
                        "promo_id": "",
                        "promo_name": valid_name,
                        "promo_value": valid_value,
                    }
                )
                continue

            if promo_id % 37 == 0:
                # 1 erreur : id_client orphelin
                writer.writerow(
                    {
                        "id_client": 999999,
                        "promo_id": promo_id,
                        "promo_name": valid_name,
                        "promo_value": valid_value,
                    }
                )
                continue

            if promo_id % 41 == 0:
                # 1 erreur : promo_name manquant
                writer.writerow(
                    {
                        "id_client": valid_client,
                        "promo_id": promo_id,
                        "promo_name": "",
                        "promo_value": valid_value,
                    }
                )
                continue

            if promo_id % 43 == 0:
                # 1 erreur : promo_value manquant
                writer.writerow(
                    {
                        "id_client": valid_client,
                        "promo_id": promo_id,
                        "promo_name": valid_name,
                        "promo_value": "",
                    }
                )
                continue

            if promo_id % 47 == 0:
                # 1 erreur : promo_value négatif
                writer.writerow(
                    {
                        "id_client": valid_client,
                        "promo_id": promo_id,
                        "promo_name": valid_name,
                        "promo_value": round(random.uniform(-50.0, -1.0), 2),
                    }
                )
                continue

            if promo_id % 53 == 0:
                # 1 erreur : promo_value trop élevé
                writer.writerow(
                    {
                        "id_client": valid_client,
                        "promo_id": promo_id,
                        "promo_name": valid_name,
                        "promo_value": round(random.uniform(1000.0, 5000.0), 2),
                    }
                )
                continue

            # Injecte parfois des lignes avec PLUSIEURS erreurs
            if promo_id % 20 == 0:
                writer.writerow(
                    {
                        "id_client": random.choice(["", -1, 999999]),
                        "promo_id": "",
                        "promo_name": random.choice(["", None, "INVALID_PROMO"]),
                        "promo_value": random.choice(["", 0.0, round(random.uniform(-200.0, -1.0), 2)]),
                    }
                )
                continue

            # Doublons exacts : duplique de temps en temps une promo existante (même promo_id)
            if promo_id % 100 == 0 and promo_id > 1:
                writer.writerow(
                    {
                        "id_client": valid_client,
                        "promo_id": promo_id - 1,
                        "promo_name": valid_name,
                        "promo_value": valid_value,
                    }
                )
                continue

            # Ligne valide
            writer.writerow(
                {
                    "id_client": valid_client,
                    "promo_id": promo_id,
                    "promo_name": valid_name,
                    "promo_value": valid_value,
                }
            )

    print(f"Generated {n_promos} promos and saved to {output_path}")


if __name__ == "__main__":
    output_dir = Path(__file__).parent.parent / "data" / "sources"

    # ✅ CHARGE / BIG DATA MODE
    # Tu peux ajuster ces valeurs selon ton PC.
    # Objectif : ~1 million de lignes par CSV.
    N_CLIENTS = 1_000_000
    N_ACHATS = 1_000_000
    N_PROMOS = 1_000_000

    max_id = generate_clients(n_clients=N_CLIENTS, output_path=output_dir / "clients.csv")
    generate_achats(max_client_id=max_id, n_achats=N_ACHATS, output_path=output_dir / "achats.csv")
    generate_promos(max_client_id=max_id, n_promos=N_PROMOS, output_path=output_dir / "promo.csv")