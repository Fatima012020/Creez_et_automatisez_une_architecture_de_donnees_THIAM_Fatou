"""
Chargement des activités sportives simulées dans PostgreSQL.

OBJECTIF
--------
Générer les activités "Strava-like" du POC puis les charger
dans la table PostgreSQL `activities`.

Le chargement est rejouable :
    - une activité nouvelle est insérée ;
    - une activité déjà présente est mise à jour ;
    - aucune activité identique n'est dupliquée.

Une activité est considérée comme unique selon :
    employee_id + start_datetime + sport_type

IMPORTANT
---------
La graine aléatoire est fixe afin de reproduire la même
simulation lors d'une nouvelle exécution dans le cadre
du POC.

Les règles utilisées ici sont les mêmes que celles validées
dans le script 06_generate_activities.py.
"""

import os
import random
from datetime import datetime, timedelta
from pathlib import Path

import psycopg
from dotenv import load_dotenv


# ============================================================
# 1. CONFIGURATION DU PROJET
# ============================================================

# Récupération du dossier racine du projet.
PROJECT_DIR = Path(__file__).resolve().parent.parent

# Chargement des variables présentes dans le fichier .env.
#
# Les identifiants PostgreSQL restent donc hors du code
# source et ne seront pas publiés sur GitHub.
load_dotenv(PROJECT_DIR / ".env")


# Configuration PostgreSQL.
DB_HOST = "localhost"
DB_PORT = os.getenv("POSTGRES_PORT", "5432")
DB_NAME = os.getenv("POSTGRES_DB")
DB_USER = os.getenv("POSTGRES_USER")
DB_PASSWORD = os.getenv("POSTGRES_PASSWORD")


# ============================================================
# 2. GRAINE ALÉATOIRE
# ============================================================

# Une graine fixe permet de reproduire la même simulation
# lors d'une nouvelle exécution.
random.seed(42)


# ============================================================
# 3. RÈGLES DE SIMULATION DES SPORTS
# ============================================================

# Pour chaque sport :
#
# duration_min / duration_max
#     Durée possible de l'activité en minutes.
#
# distance_min / distance_max
#     Distance possible en kilomètres.
#
# Une valeur None signifie que la distance n'est pas
# pertinente pour ce type d'activité.

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
# 4. COMMENTAIRES POSSIBLES
# ============================================================

# Les valeurs None sont volontaires :
# toutes les activités n'ont pas besoin d'un commentaire.

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
# 5. CONNEXION À POSTGRESQL
# ============================================================

print("=" * 60)
print("CHARGEMENT DES ACTIVITÉS STRAVA-LIKE")
print("=" * 60)

with psycopg.connect(
    host=DB_HOST,
    port=DB_PORT,
    dbname=DB_NAME,
    user=DB_USER,
    password=DB_PASSWORD,
) as connection:

    with connection.cursor() as cursor:

        print("\nConnexion PostgreSQL réussie.")


        # ====================================================
        # 6. RÉCUPÉRATION DES PROFILS SPORTIFS
        # ====================================================

        # Nous ne générons des activités que pour les salariés
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

        print(
            f"Profils sportifs récupérés : "
            f"{len(sport_profiles)}"
        )


        # ====================================================
        # 7. PÉRIODE DE SIMULATION
        # ====================================================

        # Pour l'historique du POC, on utilise minuit comme
        # borne de fin plutôt que l'heure exacte d'exécution.
        #
        # Ainsi, deux exécutions réalisées le même jour utilisent
        # exactement la même période.
        today = datetime.now().date()

        end_period = datetime.combine(
            today,
            datetime.min.time(),
        )

        start_period = end_period - timedelta(days=365)

        print(
            f"Période simulée : "
            f"{start_period.date()} -> {end_period.date()}"
        )


        # ====================================================
        # 8. GÉNÉRATION DES ACTIVITÉS
        # ====================================================

        activities = []

        for employee_id, sport_type in sport_profiles:

            # Chaque sport provenant de PostgreSQL doit
            # disposer d'une règle de simulation.
            if sport_type not in SPORT_RULES:
                raise ValueError(
                    "Aucune règle de simulation définie "
                    f"pour le sport : {sport_type}"
                )

            rule = SPORT_RULES[sport_type]

            # Entre 5 et 50 activités par salarié.
            number_of_activities = random.randint(5, 50)

            for _ in range(number_of_activities):

                # --------------------------------------------
                # DURÉE DE L'ACTIVITÉ
                # --------------------------------------------

                duration_minutes = random.randint(
                    rule["duration_min"],
                    rule["duration_max"],
                )


                # --------------------------------------------
                # DATE DE DÉBUT
                # --------------------------------------------

                # La date de début maximale tient compte de
                # la durée de CETTE activité.
                #
                # Ainsi :
                # end_datetime ne pourra jamais être située
                # dans le futur.
                latest_start = (
                    end_period
                    - timedelta(minutes=duration_minutes)
                )

                random_seconds = random.randint(
                    0,
                    int(
                        (
                            latest_start
                            - start_period
                        ).total_seconds()
                    ),
                )

                start_datetime = (
                    start_period
                    + timedelta(seconds=random_seconds)
                )


                # --------------------------------------------
                # DATE DE FIN
                # --------------------------------------------

                end_datetime = (
                    start_datetime
                    + timedelta(minutes=duration_minutes)
                )


                # --------------------------------------------
                # DISTANCE
                # --------------------------------------------

                if rule["distance_min"] is not None:

                    # Distance simulée en kilomètres.
                    distance_km = random.uniform(
                        rule["distance_min"],
                        rule["distance_max"],
                    )

                    # La note attend la distance en mètres.
                    #
                    # Un entier suffit pour le POC :
                    # 10.8 km -> environ 10800 mètres.
                    distance_meters = round(
                        distance_km * 1000
                    )

                else:

                    # Pour les sports où la distance n'est
                    # pas pertinente, PostgreSQL recevra NULL.
                    distance_meters = None


                # --------------------------------------------
                # COMMENTAIRE
                # --------------------------------------------

                comment = random.choice(COMMENTS)


                # --------------------------------------------
                # AJOUT À LA LISTE
                # --------------------------------------------

                # Nous n'ajoutons PAS activity_id :
                # PostgreSQL gérera automatiquement cet ID.
                activities.append(
                    (
                        employee_id,
                        start_datetime,
                        sport_type,
                        distance_meters,
                        end_datetime,
                        comment,
                    )
                )


        print(
            f"Activités générées : {len(activities)}"
        )


        # ====================================================
        # 9. CONTRÔLES AVANT INSERTION
        # ====================================================

        # Avant de modifier PostgreSQL, on vérifie quelques
        # règles essentielles.

        invalid_dates = 0
        negative_distances = 0
        future_activities = 0

        for activity in activities:

            (
                employee_id,
                start_datetime,
                sport_type,
                distance_meters,
                end_datetime,
                comment,
            ) = activity

            # La fin doit être strictement après le début.
            if end_datetime <= start_datetime:
                invalid_dates += 1

            # Une distance renseignée ne peut pas être négative.
            if (
                distance_meters is not None
                and distance_meters < 0
            ):
                negative_distances += 1

            # Aucune activité historique ne doit se terminer
            # après la borne de fin de la simulation.
            if end_datetime > end_period:
                future_activities += 1


        print("\nContrôles avant chargement :")
        print(
            f"- dates invalides      : {invalid_dates}"
        )
        print(
            f"- distances négatives : {negative_distances}"
        )
        print(
            f"- activités futures    : {future_activities}"
        )


        # Si une règle essentielle est violée, on arrête
        # volontairement le pipeline avant l'insertion.
        if (
            invalid_dates > 0
            or negative_distances > 0
            or future_activities > 0
        ):
            raise ValueError(
                "Contrôle qualité échoué. "
                "Chargement annulé."
            )

        # ====================================================
        # RECONSTRUCTION DE L'HISTORIQUE SIMULÉ
        # ====================================================

        # Les données générées par ce script constituent le jeu
        # historique synthétique du POC.
        #
        # À chaque exécution, cet historique est reconstruit
        # entièrement afin d'éviter les doublons.
        #
        # ATTENTION :
        # cette stratégie concerne uniquement la génération
        # historique du POC.
        #
        # Les futures activités "live" seront ajoutées par un
        # autre script et ne suivront pas cette logique.

        print(
            "\nSuppression de l'ancien historique simulé..."
        )

        cursor.execute(
            """
            TRUNCATE TABLE activities
            RESTART IDENTITY;
            """
        )

        print("Ancien historique supprimé.")


        # ====================================================
        # 10. CHARGEMENT REJOUABLE
        # ====================================================

        print("\nChargement dans activities...")


        # executemany permet d'exécuter la même requête
        # pour toutes les activités générées.
        #
        # La contrainte UNIQUE porte sur :
        #
        # employee_id + start_datetime + sport_type
        #
        # Si cette activité n'existe pas :
        #     INSERT
        #
        # Si elle existe déjà :
        #     UPDATE
        #
        # Cela évite la création de doublons.
        cursor.executemany(
            """
            INSERT INTO activities (
                employee_id,
                start_datetime,
                sport_type,
                distance_meters,
                end_datetime,
                comment
            )
            VALUES (
                %s, %s, %s, %s, %s, %s
            )

            ON CONFLICT (
                employee_id,
                start_datetime,
                sport_type
            )
            DO UPDATE SET
                distance_meters =
                    EXCLUDED.distance_meters,

                end_datetime =
                    EXCLUDED.end_datetime,

                comment =
                    EXCLUDED.comment;
            """,
            activities,
        )


        # ====================================================
        # 11. VÉRIFICATION APRÈS CHARGEMENT
        # ====================================================

        cursor.execute(
            """
            SELECT COUNT(*)
            FROM activities;
            """
        )

        total_activities = cursor.fetchone()[0]


        # Nombre de salariés possédant au moins
        # une activité.
        cursor.execute(
            """
            SELECT COUNT(DISTINCT employee_id)
            FROM activities;
            """
        )

        employees_with_activity = cursor.fetchone()[0]


        # Nombre de sports présents.
        cursor.execute(
            """
            SELECT COUNT(DISTINCT sport_type)
            FROM activities;
            """
        )

        sports_count = cursor.fetchone()[0]


# ============================================================
# 12. RÉSUMÉ FINAL
# ============================================================

print("\n" + "=" * 60)
print("RÉSUMÉ DU CHARGEMENT")
print("=" * 60)

print(
    f"Activités générées       : {len(activities)}"
)

print(
    f"Activités en PostgreSQL  : {total_activities}"
)

print(
    f"Salariés avec activité   : {employees_with_activity}"
)

print(
    f"Sports différents        : {sports_count}"
)

print("\nChargement terminé avec succès.")