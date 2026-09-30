"""
Génération de données d'activités sportives "Strava-like".

OBJECTIF
-------
Créer plusieurs milliers d'activités sportives simulées
sur les 12 derniers mois à partir des pratiques sportives
déclarées par les salariés.

À cette étape :
    - les profils sportifs sont lus depuis PostgreSQL ;
    - les activités sont générées en mémoire ;
    - plusieurs contrôles sont réalisés ;
    - AUCUNE activité n'est encore enregistrée en base.

La génération utilise une graine aléatoire fixe afin
d'obtenir des résultats reproductibles.
"""

import os
import random
from datetime import datetime, timedelta
from pathlib import Path

import pandas as pd
import psycopg
from dotenv import load_dotenv


# ============================================================
# 1. CONFIGURATION
# ============================================================

# Racine du projet.
PROJECT_DIR = Path(__file__).resolve().parent.parent

# Chargement des variables PostgreSQL depuis .env.
load_dotenv(PROJECT_DIR / ".env")

DB_HOST = "localhost"
DB_PORT = os.getenv("POSTGRES_PORT", "5432")
DB_NAME = os.getenv("POSTGRES_DB")
DB_USER = os.getenv("POSTGRES_USER")
DB_PASSWORD = os.getenv("POSTGRES_PASSWORD")


# ------------------------------------------------------------
# Graine aléatoire
# ------------------------------------------------------------
# Une graine fixe permet de reproduire exactement
# la même simulation lors d'une nouvelle exécution.
random.seed(42)


# ============================================================
# 2. RÈGLES DE SIMULATION
# ============================================================

# Pour chaque sport :
#
# duration_min / duration_max :
#     durée possible de l'activité en minutes.
#
# distance_min / distance_max :
#     distance possible en kilomètres.
#
# Lorsque les distances sont à None, la distance n'est
# pas considérée comme pertinente pour ce sport.

SPORT_RULES = {
    "Running": {
        "duration_min": 20,
        "duration_max": 120,
        "distance_min": 2,
        "distance_max": 25,
    },

    "Randonnée": {
        "duration_min": 60,
        "duration_max": 480,
        "distance_min": 3,
        "distance_max": 30,
    },

    "Tennis": {
        "duration_min": 45,
        "duration_max": 180,
        "distance_min": None,
        "distance_max": None,
    },

    "Natation": {
        "duration_min": 20,
        "duration_max": 120,
        "distance_min": 0.5,
        "distance_max": 5,
    },

    "Football": {
        "duration_min": 45,
        "duration_max": 120,
        "distance_min": None,
        "distance_max": None,
    },

    "Rugby": {
        "duration_min": 45,
        "duration_max": 120,
        "distance_min": None,
        "distance_max": None,
    },

    "Badminton": {
        "duration_min": 30,
        "duration_max": 120,
        "distance_min": None,
        "distance_max": None,
    },

    "Voile": {
        "duration_min": 60,
        "duration_max": 360,
        "distance_min": None,
        "distance_max": None,
    },

    "Boxe": {
        "duration_min": 30,
        "duration_max": 120,
        "distance_min": None,
        "distance_max": None,
    },

    "Judo": {
        "duration_min": 30,
        "duration_max": 120,
        "distance_min": None,
        "distance_max": None,
    },

    "Escalade": {
        "duration_min": 45,
        "duration_max": 240,
        "distance_min": None,
        "distance_max": None,
    },

    "Triathlon": {
        "duration_min": 60,
        "duration_max": 300,
        "distance_min": 10,
        "distance_max": 80,
    },

    "Équitation": {
        "duration_min": 45,
        "duration_max": 180,
        "distance_min": None,
        "distance_max": None,
    },

    "Tennis de table": {
        "duration_min": 30,
        "duration_max": 120,
        "distance_min": None,
        "distance_max": None,
    },

    "Basketball": {
        "duration_min": 45,
        "duration_max": 120,
        "distance_min": None,
        "distance_max": None,
    },
}


# ============================================================
# 3. COMMENTAIRES POSSIBLES
# ============================================================

# None est volontairement présent plusieurs fois :
# la majorité des activités n'auront pas forcément
# de commentaire.

COMMENTS = [
    None,
    None,
    None,
    None,
    "Belle séance !",
    "Reprise du sport :)",
    "Sortie avec des amis",
    "Bonne séance aujourd'hui",
    "Objectif atteint !",
]


# ============================================================
# 4. LECTURE DES PROFILS SPORTIFS
# ============================================================

print("=" * 60)
print("GÉNÉRATION DES ACTIVITÉS STRAVA-LIKE")
print("=" * 60)

with psycopg.connect(
    host=DB_HOST,
    port=DB_PORT,
    dbname=DB_NAME,
    user=DB_USER,
    password=DB_PASSWORD,
) as connection:

    with connection.cursor() as cursor:

        # Nous sélectionnons uniquement les salariés
        # ayant une pratique sportive renseignée.
        cursor.execute(
            """
            SELECT
                employee_id,
                sport_type
            FROM sport_profile
            WHERE sport_type IS NOT NULL
            ORDER BY employee_id;
            """
        )

        sport_profiles = cursor.fetchall()


print(f"\nProfils sportifs récupérés : {len(sport_profiles)}")


# ============================================================
# 5. DÉFINITION DE LA PÉRIODE
# ============================================================

# Date actuelle.
end_period = datetime.now()

# Historique de 365 jours.
start_period = end_period - timedelta(days=365)

print(
    f"Période simulée : "
    f"{start_period.date()} → {end_period.date()}"
)


# ============================================================
# 6. GÉNÉRATION DES ACTIVITÉS
# ============================================================

activities = []

# Compteur utilisé uniquement pour identifier les activités
# simulées en mémoire.
activity_id = 1


for employee_id, sport_type in sport_profiles:

    # Vérification de sécurité :
    # chaque sport provenant de PostgreSQL doit avoir
    # une règle de simulation.
    if sport_type not in SPORT_RULES:
        raise ValueError(
            f"Aucune règle définie pour le sport : {sport_type}"
        )

    rule = SPORT_RULES[sport_type]

    # Chaque salarié reçoit entre 5 et 50 activités.
    number_of_activities = random.randint(5, 50)

    for _ in range(number_of_activities):

        # ----------------------------------------------------
        # DATE DE DÉBUT
        # ----------------------------------------------------

        # On retire la durée maximale possible afin d'éviter
        # qu'une activité générée se termine dans le futur.
        latest_start = end_period - timedelta(
            minutes=rule["duration_max"]
        )

        random_seconds = random.randint(
            0,
            int((latest_start - start_period).total_seconds()),
        )

        start_datetime = (
            start_period
            + timedelta(seconds=random_seconds)
        )


        # ----------------------------------------------------
        # DURÉE
        # ----------------------------------------------------

        duration_minutes = random.randint(
            rule["duration_min"],
            rule["duration_max"],
        )

        end_datetime = (
            start_datetime
            + timedelta(minutes=duration_minutes)
        )


        # ----------------------------------------------------
        # DISTANCE
        # ----------------------------------------------------

        # Certains sports ont une distance pertinente.
        if rule["distance_min"] is not None:

            distance_km = random.uniform(
                rule["distance_min"],
                rule["distance_max"],
            )

            # PostgreSQL attend une distance en mètres.
            distance_meters = round(
                distance_km * 1000,
                2,
            )

        else:

            # Pour les sports où la distance n'est pas
            # pertinente, nous conservons une valeur vide.
            distance_meters = None


        # ----------------------------------------------------
        # COMMENTAIRE
        # ----------------------------------------------------

        comment = random.choice(COMMENTS)


        # ----------------------------------------------------
        # CRÉATION DE L'ACTIVITÉ
        # ----------------------------------------------------

        activities.append(
            {
                "activity_id": activity_id,
                "employee_id": employee_id,
                "start_datetime": start_datetime,
                "sport_type": sport_type,
                "distance_meters": distance_meters,
                "end_datetime": end_datetime,
                "comment": comment,
            }
        )

        activity_id += 1


# ============================================================
# 7. CONVERSION EN DATAFRAME
# ============================================================

# Le DataFrame facilite l'affichage et les contrôles.
df_activities = pd.DataFrame(activities)


# ============================================================
# 8. RÉSUMÉ
# ============================================================

print("\n" + "=" * 60)
print("RÉSUMÉ DE LA GÉNÉRATION")
print("=" * 60)

print(f"Nombre de salariés sportifs : {len(sport_profiles)}")
print(f"Nombre d'activités générées : {len(df_activities)}")

print(
    f"Nombre de sports différents : "
    f"{df_activities['sport_type'].nunique()}"
)


# ============================================================
# 9. CONTRÔLES SIMPLES
# ============================================================

print("\n" + "=" * 60)
print("CONTRÔLES")
print("=" * 60)


# ------------------------------------------------------------
# Contrôle 1 : dates
# ------------------------------------------------------------

invalid_dates = df_activities[
    df_activities["end_datetime"]
    <= df_activities["start_datetime"]
]

print(
    f"Activités avec date invalide : "
    f"{len(invalid_dates)}"
)


# ------------------------------------------------------------
# Contrôle 2 : distances négatives
# ------------------------------------------------------------

negative_distances = df_activities[
    df_activities["distance_meters"].notna()
    & (df_activities["distance_meters"] < 0)
]

print(
    f"Distances négatives : "
    f"{len(negative_distances)}"
)


# ------------------------------------------------------------
# Contrôle 3 : période
# ------------------------------------------------------------

outside_period = df_activities[
    (df_activities["start_datetime"] < start_period)
    | (df_activities["start_datetime"] > end_period)
]

print(
    f"Activités hors période : "
    f"{len(outside_period)}"
)


# ============================================================
# 10. NOMBRE D'ACTIVITÉS PAR SALARIÉ
# ============================================================

activities_per_employee = (
    df_activities
    .groupby("employee_id")
    .size()
)

print("\nActivités par salarié :")

print(
    f"- minimum : {activities_per_employee.min()}"
)

print(
    f"- maximum : {activities_per_employee.max()}"
)

print(
    f"- moyenne : {activities_per_employee.mean():.2f}"
)


# ============================================================
# 11. TEST DU SEUIL DES 15 ACTIVITÉS
# ============================================================

# Cela permet déjà de vérifier que notre simulation
# produira les deux situations nécessaires au futur
# calcul des journées bien-être.

below_15 = (activities_per_employee < 15).sum()
at_least_15 = (activities_per_employee >= 15).sum()

print("\nSeuil des 15 activités :")

print(
    f"- salariés avec moins de 15 activités : "
    f"{below_15}"
)

print(
    f"- salariés avec au moins 15 activités : "
    f"{at_least_15}"
)


# ============================================================
# 12. RÉPARTITION PAR SPORT
# ============================================================

print("\nRépartition des activités par sport :")

print(
    df_activities["sport_type"]
    .value_counts()
)


# ============================================================
# 13. EXEMPLES D'ACTIVITÉS
# ============================================================

print("\n" + "=" * 60)
print("EXEMPLE DES 10 PREMIÈRES ACTIVITÉS")
print("=" * 60)

print(
    df_activities.head(10).to_string(index=False)
)


# ============================================================
# 14. FIN
# ============================================================

print("\n" + "=" * 60)
print("GÉNÉRATION TERMINÉE")
print("=" * 60)

print(
    "Les activités sont uniquement en mémoire : "
    "aucune donnée n'a été insérée dans PostgreSQL."
)