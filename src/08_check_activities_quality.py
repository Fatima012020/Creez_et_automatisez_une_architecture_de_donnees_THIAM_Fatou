"""
Contrôle qualité des activités sportives stockées dans PostgreSQL.

OBJECTIF
--------
Vérifier que les données réellement enregistrées dans la table
`activities` sont cohérentes avant de poursuivre le pipeline.

Les contrôles portent notamment sur :
    - le volume de données ;
    - les doublons ;
    - les relations avec les salariés ;
    - la cohérence des dates ;
    - les distances ;
    - les types de sport ;
    - la couverture des 12 derniers mois ;
    - le seuil métier des 15 activités.

Ce script est uniquement un script de contrôle :
il ne modifie aucune donnée PostgreSQL.
"""

import os
from pathlib import Path

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


# ============================================================
# 2. CONNEXION À POSTGRESQL
# ============================================================

print("=" * 60)
print("CONTRÔLE QUALITÉ DE LA TABLE ACTIVITIES")
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
        # 3. VOLUME GLOBAL
        # ====================================================

        cursor.execute(
            """
            SELECT COUNT(*)
            FROM activities;
            """
        )

        total_activities = cursor.fetchone()[0]


        # ====================================================
        # 4. NOMBRE DE SALARIÉS
        # ====================================================

        cursor.execute(
            """
            SELECT COUNT(DISTINCT employee_id)
            FROM activities;
            """
        )

        employees_with_activity = cursor.fetchone()[0]


        # ====================================================
        # 5. NOMBRE DE SPORTS
        # ====================================================

        cursor.execute(
            """
            SELECT COUNT(DISTINCT sport_type)
            FROM activities;
            """
        )

        sports_count = cursor.fetchone()[0]


        # ====================================================
        # 6. DOUBLONS
        # ====================================================

        # Notre règle d'unicité considère qu'une activité
        # est identifiée par :
        #
        # employee_id + start_datetime + sport_type

        cursor.execute(
            """
            SELECT COUNT(*)
            FROM (
                SELECT
                    employee_id,
                    start_datetime,
                    sport_type,
                    COUNT(*) AS nb
                FROM activities
                GROUP BY
                    employee_id,
                    start_datetime,
                    sport_type
                HAVING COUNT(*) > 1
            ) AS duplicates;
            """
        )

        duplicate_count = cursor.fetchone()[0]


        # ====================================================
        # 7. CLÉS ÉTRANGÈRES / SALARIÉS
        # ====================================================

        # Vérifie qu'aucune activité ne référence un salarié
        # absent de la table employees.

        cursor.execute(
            """
            SELECT COUNT(*)
            FROM activities a
            LEFT JOIN employees e
                ON a.employee_id = e.employee_id
            WHERE e.employee_id IS NULL;
            """
        )

        unknown_employees = cursor.fetchone()[0]


        # ====================================================
        # 8. DATES INCOHÉRENTES
        # ====================================================

        # Une activité doit se terminer après son début.

        cursor.execute(
            """
            SELECT COUNT(*)
            FROM activities
            WHERE end_datetime <= start_datetime;
            """
        )

        invalid_dates = cursor.fetchone()[0]


        # ====================================================
        # 9. ACTIVITÉS FUTURES
        # ====================================================

        cursor.execute(
            """
            SELECT COUNT(*)
            FROM activities
            WHERE end_datetime > CURRENT_TIMESTAMP;
            """
        )

        future_activities = cursor.fetchone()[0]


        # ====================================================
        # 10. HISTORIQUE DE PLUS DE 12 MOIS
        # ====================================================

        # Le générateur historique utilise le jour courant à
        # minuit comme borne de référence.
        #
        # Le contrôle applique donc exactement la même règle
        # afin d'éviter qu'une activité du premier jour soit
        # considérée à tort comme trop ancienne simplement
        # à cause de l'heure d'exécution du script.

        cursor.execute(
            """
            SELECT COUNT(*)
            FROM activities
            WHERE start_datetime
                < DATE_TRUNC('day', CURRENT_TIMESTAMP)
                    - INTERVAL '12 months';
            """
        )

        too_old_activities = cursor.fetchone()[0]


        # ====================================================
        # 11. DISTANCES NÉGATIVES
        # ====================================================

        # NULL est autorisé.
        # En revanche, une distance renseignée ne doit
        # jamais être négative.

        cursor.execute(
            """
            SELECT COUNT(*)
            FROM activities
            WHERE distance_meters < 0;
            """
        )

        negative_distances = cursor.fetchone()[0]


        # ====================================================
        # 12. DISTANCES NULL
        # ====================================================

        # Une distance NULL n'est pas forcément une anomalie.
        # Certains sports ne nécessitent pas de distance.

        cursor.execute(
            """
            SELECT COUNT(*)
            FROM activities
            WHERE distance_meters IS NULL;
            """
        )

        null_distances = cursor.fetchone()[0]


        # ====================================================
        # 13. SPORTS INCOHÉRENTS
        # ====================================================

        # Dans notre simulation, le sport de l'activité doit
        # correspondre au sport déclaré dans sport_profile.

        cursor.execute(
            """
            SELECT COUNT(*)
            FROM activities a
            JOIN sport_profile sp
                ON a.employee_id = sp.employee_id
            WHERE a.sport_type <> sp.sport_type;
            """
        )

        inconsistent_sports = cursor.fetchone()[0]


        # ====================================================
        # 14. ACTIVITÉS SANS PROFIL SPORTIF
        # ====================================================

        cursor.execute(
            """
            SELECT COUNT(*)
            FROM activities a
            LEFT JOIN sport_profile sp
                ON a.employee_id = sp.employee_id
            WHERE sp.employee_id IS NULL
               OR sp.sport_type IS NULL;
            """
        )

        activities_without_profile = cursor.fetchone()[0]


        # ====================================================
        # 15. MINIMUM ET MAXIMUM DES DATES
        # ====================================================

        cursor.execute(
            """
            SELECT
                MIN(start_datetime),
                MAX(end_datetime)
            FROM activities;
            """
        )

        min_date, max_date = cursor.fetchone()


        # ====================================================
        # 16. SEUIL DES 15 ACTIVITÉS
        # ====================================================

        # La note de cadrage utilise 15 activités dans
        # l'année comme seuil pour les journées bien-être.

        cursor.execute(
            """
            SELECT
                COUNT(*) FILTER (
                    WHERE activity_count < 15
                ),
                COUNT(*) FILTER (
                    WHERE activity_count >= 15
                )
            FROM (
                SELECT
                    employee_id,
                    COUNT(*) AS activity_count
                FROM activities
                GROUP BY employee_id
            ) AS employee_activity_counts;
            """
        )

        below_15, at_least_15 = cursor.fetchone()


        # ====================================================
        # 17. RÉPARTITION PAR SPORT
        # ====================================================

        cursor.execute(
            """
            SELECT
                sport_type,
                COUNT(*) AS activity_count
            FROM activities
            GROUP BY sport_type
            ORDER BY activity_count DESC;
            """
        )

        sport_distribution = cursor.fetchall()


        # ====================================================
        # 18. RÉPARTITION MENSUELLE
        # ====================================================

        # Ce contrôle permet de vérifier que la génération
        # couvre bien l'ensemble de la période au lieu
        # d'être concentrée sur quelques jours.

        cursor.execute(
            """
            SELECT
                TO_CHAR(
                    DATE_TRUNC('month', start_datetime),
                    'YYYY-MM'
                ) AS month,
                COUNT(*) AS activity_count
            FROM activities
            GROUP BY DATE_TRUNC('month', start_datetime)
            ORDER BY DATE_TRUNC('month', start_datetime);
            """
        )

        monthly_distribution = cursor.fetchall()


# ============================================================
# 19. AFFICHAGE DU RÉSUMÉ
# ============================================================

print("\n" + "=" * 60)
print("RÉSUMÉ GÉNÉRAL")
print("=" * 60)

print(f"Activités                : {total_activities}")
print(f"Salariés représentés     : {employees_with_activity}")
print(f"Sports différents        : {sports_count}")

print(f"\nPremière activité        : {min_date}")
print(f"Dernière activité        : {max_date}")


# ============================================================
# 20. AFFICHAGE DES CONTRÔLES QUALITÉ
# ============================================================

print("\n" + "=" * 60)
print("CONTRÔLES QUALITÉ")
print("=" * 60)

print(f"Doublons                 : {duplicate_count}")
print(f"Salariés inconnus        : {unknown_employees}")
print(f"Dates invalides          : {invalid_dates}")
print(f"Activités futures        : {future_activities}")
print(f"Activités > 12 mois      : {too_old_activities}")
print(f"Distances négatives      : {negative_distances}")
print(f"Distances NULL           : {null_distances}")
print(f"Sports incohérents       : {inconsistent_sports}")
print(
    f"Activités sans profil    : "
    f"{activities_without_profile}"
)


# ============================================================
# 21. SEUIL DES 15 ACTIVITÉS
# ============================================================

print("\n" + "=" * 60)
print("SEUIL DES 15 ACTIVITÉS")
print("=" * 60)

print(
    f"Salariés avec < 15 activités  : {below_15}"
)

print(
    f"Salariés avec >= 15 activités : {at_least_15}"
)


# ============================================================
# 22. RÉPARTITION PAR SPORT
# ============================================================

print("\n" + "=" * 60)
print("RÉPARTITION PAR SPORT")
print("=" * 60)

for sport_type, count in sport_distribution:
    print(
        f"{sport_type:<20} : {count}"
    )


# ============================================================
# 23. RÉPARTITION MENSUELLE
# ============================================================

print("\n" + "=" * 60)
print("RÉPARTITION MENSUELLE")
print("=" * 60)

for month, count in monthly_distribution:
    print(
        f"{month} : {count}"
    )


# ============================================================
# 24. VERDICT AUTOMATIQUE
# ============================================================

# Les distances NULL ne sont volontairement PAS incluses
# dans les erreurs car elles sont légitimes pour certains
# sports.

quality_errors = (
    duplicate_count
    + unknown_employees
    + invalid_dates
    + future_activities
    + too_old_activities
    + negative_distances
    + inconsistent_sports
    + activities_without_profile
)

print("\n" + "=" * 60)
print("VERDICT")
print("=" * 60)

if quality_errors == 0:
    print("QUALITÉ OK : aucun problème bloquant détecté.")
else:
    print(
        f"QUALITÉ KO : "
        f"{quality_errors} problème(s) détecté(s)."
    )