"""
10_build_employee_benefits.py

Étape 8.10.1 - Sport Data Solution

Objectif :
    Construire avec Spark le dataset métier employee_benefits
    à partir de :

        - PostgreSQL public.employees
        - PostgreSQL public.commute_validation
        - Delta Lake activities

À cette étape :
    - lecture des sources ;
    - agrégation des activités ;
    - jointures ;
    - application des règles métier ;
    - contrôles ;
    - affichage du résultat.

IMPORTANT :
    Aucune écriture dans Delta Lake n'est réalisée ici.

Granularité finale :
    1 ligne = 1 salarié

Règles métier :
    1. Prime sportive :
       trajet sportif ET trajet validé
       -> 5 % du salaire brut annuel.

    2. Journées bien-être :
       au moins 15 activités
       -> 5 journées.

Valeurs de référence :
    - 161 salariés
    - 68 éligibles à la prime
    - 172 482,50 € de primes
    - 75 éligibles aux journées bien-être
    - 375 journées attribuées
"""

import os

from pyspark.sql import SparkSession
from pyspark.sql import functions as F


# ============================================================
# PARAMÈTRES MÉTIER
# ============================================================

SPORT_BONUS_RATE = 0.05
MIN_ACTIVITIES = 15
WELLBEING_DAYS = 5


# ============================================================
# CONFIGURATION POSTGRESQL
# ============================================================

POSTGRES_HOST = "postgres"
POSTGRES_PORT = "5432"
POSTGRES_DB = "sport_data_db"
POSTGRES_USER = "sport_user"

# Le mot de passe PostgreSQL est récupéré depuis les variables
# d'environnement afin de ne jamais stocker de secret dans Git.
POSTGRES_PASSWORD = os.getenv("POSTGRES_PASSWORD")

if not POSTGRES_PASSWORD:
    raise ValueError(
        "La variable d'environnement POSTGRES_PASSWORD est absente."
    )

JDBC_URL = (
    f"jdbc:postgresql://"
    f"{POSTGRES_HOST}:"
    f"{POSTGRES_PORT}/"
    f"{POSTGRES_DB}"
)


# ============================================================
# CONFIGURATION DELTA
# ============================================================

ACTIVITIES_DELTA_PATH = "/opt/spark/work-dir/delta/activities"

EMPLOYEE_BENEFITS_DELTA_PATH = (
    "/opt/spark/work-dir/delta/employee_benefits"
)


# ============================================================
# SESSION SPARK
# ============================================================

spark = (
    SparkSession.builder
    .appName("SportData-Build-Employee-Benefits")
    .config(
        "spark.sql.extensions",
        "io.delta.sql.DeltaSparkSessionExtension"
    )
    .config(
        "spark.sql.catalog.spark_catalog",
        "org.apache.spark.sql.delta.catalog.DeltaCatalog"
    )
    .getOrCreate()
)

spark.sparkContext.setLogLevel("WARN")


print("=" * 60)
print("CONSTRUCTION DE EMPLOYEE_BENEFITS")
print("=" * 60)


# ============================================================
# 1. LECTURE DE EMPLOYEES
# ============================================================

print()
print("Lecture de public.employees...")

employees = (
    spark.read
    .format("jdbc")
    .option("url", JDBC_URL)
    .option("dbtable", "public.employees")
    .option("user", POSTGRES_USER)
    .option("password", POSTGRES_PASSWORD)
    .option("driver", "org.postgresql.Driver")
    .load()
)

print(f"Salariés lus : {employees.count()}")


# ============================================================
# 2. LECTURE DE COMMUTE_VALIDATION
# ============================================================

print()
print("Lecture de public.commute_validation...")

commute = (
    spark.read
    .format("jdbc")
    .option("url", JDBC_URL)
    .option("dbtable", "public.commute_validation")
    .option("user", POSTGRES_USER)
    .option("password", POSTGRES_PASSWORD)
    .option("driver", "org.postgresql.Driver")
    .load()
)

print(f"Validations trajet lues : {commute.count()}")


# ============================================================
# 3. LECTURE DE DELTA ACTIVITIES
# ============================================================

print()
print("Lecture de Delta activities...")

activities = (
    spark.read
    .format("delta")
    .load(ACTIVITIES_DELTA_PATH)
)

print(f"Activités Delta lues : {activities.count()}")

# ============================================================
# ACTIVITÉS DES 12 DERNIERS MOIS
# ============================================================
#
# Pour l'attribution des journées bien-être, seules les
# activités réalisées au cours des 12 derniers mois sont
# prises en compte.
#
# La période est calculée dynamiquement à partir de la date
# d'exécution du traitement.

activities_last_12_months = (
    activities
    .filter(
        F.col("start_datetime")
        >= F.add_months(F.current_date(), -12)
    )
)


# ============================================================
# AGRÉGATION DES ACTIVITÉS PAR SALARIÉ
# ============================================================

activity_summary = (
    activities_last_12_months
    .groupBy("employee_id")
    .agg(
        F.count("activity_id").alias("activity_count"),

        F.sum("distance_meters").alias(
            "total_distance_meters"
        )
    )
)

print()
print(
    "Salariés ayant au moins une activité : "
    f"{activity_summary.count()}"
)


# ============================================================
# 5. PRÉPARATION DES VALIDATIONS TRAJET
# ============================================================
#
# On ne conserve que les colonnes utiles au calcul final.
#
# Les salariés absents de cette table seront conservés grâce
# au LEFT JOIN réalisé depuis employees.
# ============================================================

commute_selected = (
    commute
    .select(
        "employee_id",
        "distance_km",
        "is_sport_commute",
        "is_valid"
    )
)


# ============================================================
# 6. JOINTURES
# ============================================================
#
# employees est la table principale.
#
# C'est indispensable pour conserver les 161 salariés,
# y compris :
#
# - les salariés sans validation de trajet ;
# - les salariés sans activité sportive.
#
# On utilise donc deux LEFT JOIN.
# ============================================================

joined = (
    employees.alias("e")

    .join(
        commute_selected.alias("c"),
        F.col("e.employee_id") == F.col("c.employee_id"),
        "left"
    )

    .join(
        activity_summary.alias("a"),
        F.col("e.employee_id") == F.col("a.employee_id"),
        "left"
    )
)


# ============================================================
# 7. CONSTRUCTION DES COLONNES MÉTIER
# ============================================================

employee_benefits = (
    joined

    # --------------------------------------------------------
    # Colonnes principales
    # --------------------------------------------------------

    .select(
        F.col("e.employee_id").alias("employee_id"),
        F.col("e.first_name").alias("first_name"),
        F.col("e.last_name").alias("last_name"),
        F.col("e.business_unit").alias("business_unit"),
        F.col("e.gross_salary").alias("gross_salary"),
        F.col("e.commute_mode").alias("commute_mode"),

        F.col("c.distance_km").alias(
            "commute_distance_km"
        ),

        # Un salarié absent de commute_validation
        # est considéré comme n'ayant pas de trajet sportif
        # validé dans le POC.
        F.coalesce(
            F.col("c.is_sport_commute"),
            F.lit(False)
        ).alias("is_sport_commute"),

        F.coalesce(
            F.col("c.is_valid"),
            F.lit(False)
        ).alias("commute_validated"),

        # Un salarié sans activité obtient 0.
        F.coalesce(
            F.col("a.activity_count"),
            F.lit(0)
        ).cast("int").alias("activity_count"),

        F.coalesce(
            F.col("a.total_distance_meters"),
            F.lit(0)
        ).alias("total_distance_meters")
    )


    # ========================================================
    # PRIME SPORTIVE
    # ========================================================

    .withColumn(
        "sport_bonus_eligible",

        F.col("is_sport_commute")
        & F.col("commute_validated")
    )

    .withColumn(
        "sport_bonus_amount",

        F.when(
            F.col("sport_bonus_eligible"),

            F.round(
                F.col("gross_salary")
                * F.lit(SPORT_BONUS_RATE),
                2
            )
        )
        .otherwise(
            F.lit(0.00)
        )
    )


    # ========================================================
    # JOURNÉES BIEN-ÊTRE
    # ========================================================

    .withColumn(
        "wellbeing_days_eligible",

        F.col("activity_count") >= MIN_ACTIVITIES
    )

    .withColumn(
        "wellbeing_days_awarded",

        F.when(
            F.col("wellbeing_days_eligible"),
            F.lit(WELLBEING_DAYS)
        )
        .otherwise(
            F.lit(0)
        )
    )


    # ========================================================
    # DATE DU CALCUL
    # ========================================================

    .withColumn(
        "calculation_date",
        F.current_date()
    )
)


# ============================================================
# 8. AFFICHAGE DU SCHÉMA FINAL
# ============================================================

print()
print("=" * 60)
print("SCHÉMA EMPLOYEE_BENEFITS")
print("=" * 60)

employee_benefits.printSchema()


# ============================================================
# 9. CONTRÔLES GÉNÉRAUX
# ============================================================

total_rows = employee_benefits.count()

distinct_employees = (
    employee_benefits
    .select("employee_id")
    .distinct()
    .count()
)

duplicates = total_rows - distinct_employees


# ============================================================
# 10. CONTRÔLE PRIME SPORTIVE
# ============================================================

eligible_bonus = (
    employee_benefits
    .filter(F.col("sport_bonus_eligible"))
    .count()
)

non_eligible_bonus = (
    employee_benefits
    .filter(~F.col("sport_bonus_eligible"))
    .count()
)

total_bonus = (
    employee_benefits
    .agg(
        F.round(
            F.sum("sport_bonus_amount"),
            2
        ).alias("total")
    )
    .first()["total"]
)


# ============================================================
# 11. CONTRÔLE JOURNÉES BIEN-ÊTRE
# ============================================================

eligible_wellbeing = (
    employee_benefits
    .filter(F.col("wellbeing_days_eligible"))
    .count()
)

non_eligible_wellbeing = (
    employee_benefits
    .filter(~F.col("wellbeing_days_eligible"))
    .count()
)

total_wellbeing_days = (
    employee_benefits
    .agg(
        F.sum("wellbeing_days_awarded").alias("total")
    )
    .first()["total"]
)


# ============================================================
# 12. RÉSULTATS
# ============================================================

print()
print("=" * 60)
print("CONTRÔLES EMPLOYEE_BENEFITS")
print("=" * 60)

print(f"Lignes                         : {total_rows}")
print(f"Employee ID distincts          : {distinct_employees}")
print(f"Doublons employee_id           : {duplicates}")

print()

print(f"Éligibles prime sportive       : {eligible_bonus}")
print(f"Non éligibles prime            : {non_eligible_bonus}")
print(f"Montant total primes           : {total_bonus} €")

print()

print(f"Éligibles journées bien-être   : {eligible_wellbeing}")
print(f"Non éligibles journées         : {non_eligible_wellbeing}")
print(f"Journées bien-être attribuées  : {total_wellbeing_days}")


# ============================================================
# 13. AFFICHAGE D'UN ÉCHANTILLON
# ============================================================

print()
print("=" * 60)
print("APERÇU EMPLOYEE_BENEFITS")
print("=" * 60)

(
    employee_benefits
    .select(
        "employee_id",
        "first_name",
        "last_name",
        "business_unit",
        "gross_salary",
        "sport_bonus_eligible",
        "sport_bonus_amount",
        "activity_count",
        "wellbeing_days_eligible",
        "wellbeing_days_awarded"
    )
    .orderBy("employee_id")
    .show(
        20,
        truncate=False
    )
)


# ============================================================
# 14. VERDICT
# ============================================================

print()
print("=" * 60)
print("VERDICT")
print("=" * 60)


EXPECTED_EMPLOYEES = 161
EXPECTED_BONUS_ELIGIBLE = 68
EXPECTED_TOTAL_BONUS = 172482.50
EXPECTED_WELLBEING_ELIGIBLE = 73
EXPECTED_WELLBEING_DAYS = 365


checks = [
    total_rows == EXPECTED_EMPLOYEES,
    distinct_employees == EXPECTED_EMPLOYEES,
    duplicates == 0,

    eligible_bonus == EXPECTED_BONUS_ELIGIBLE,

    abs(
        float(total_bonus)
        - EXPECTED_TOTAL_BONUS
    ) < 0.01,

    eligible_wellbeing
    == EXPECTED_WELLBEING_ELIGIBLE,

    total_wellbeing_days
    == EXPECTED_WELLBEING_DAYS
]


if all(checks):

    print("CALCUL MÉTIER OK.")
    print("Toutes les valeurs de référence sont retrouvées.")

else:

    print("CALCUL MÉTIER KO.")
    print(
        "Au moins une valeur diffère "
        "des contrôles de référence."
    )

# ============================================================
# 15. ÉCRITURE DANS DELTA LAKE
# ============================================================
#
# employee_benefits est une table de résultat métier :
#
#     1 ligne = 1 salarié
#
# Le calcul est reconstruit entièrement à partir des sources
# à chaque exécution.
#
# Le mode overwrite permet donc de remplacer proprement
# l'état précédent sans accumuler de doublons.
# ============================================================

print()
print("=" * 60)
print("ÉCRITURE DELTA - EMPLOYEE_BENEFITS")
print("=" * 60)

print(f"Destination : {EMPLOYEE_BENEFITS_DELTA_PATH}")

(
    employee_benefits
    .write
    .format("delta")
    .mode("overwrite")
    .option("overwriteSchema", "true")
    .save(EMPLOYEE_BENEFITS_DELTA_PATH)
)

print("Écriture Delta terminée.")


# ============================================================
# 16. RELECTURE DE CONTRÔLE
# ============================================================

print()
print("=" * 60)
print("CONTRÔLE APRÈS ÉCRITURE")
print("=" * 60)

delta_benefits = (
    spark.read
    .format("delta")
    .load(EMPLOYEE_BENEFITS_DELTA_PATH)
)

delta_rows = delta_benefits.count()

delta_distinct_employees = (
    delta_benefits
    .select("employee_id")
    .distinct()
    .count()
)

delta_duplicates = (
    delta_rows - delta_distinct_employees
)

delta_bonus_eligible = (
    delta_benefits
    .filter(F.col("sport_bonus_eligible"))
    .count()
)

delta_total_bonus = (
    delta_benefits
    .agg(
        F.round(
            F.sum("sport_bonus_amount"),
            2
        ).alias("total")
    )
    .first()["total"]
)

delta_wellbeing_eligible = (
    delta_benefits
    .filter(F.col("wellbeing_days_eligible"))
    .count()
)

delta_total_wellbeing_days = (
    delta_benefits
    .agg(
        F.sum("wellbeing_days_awarded").alias("total")
    )
    .first()["total"]
)


print(f"Lignes                       : {delta_rows}")
print(
    f"Employee ID distincts        : "
    f"{delta_distinct_employees}"
)
print(f"Doublons employee_id         : {delta_duplicates}")

print()

print(
    f"Éligibles prime sportive     : "
    f"{delta_bonus_eligible}"
)
print(
    f"Montant total primes         : "
    f"{delta_total_bonus} €"
)

print()

print(
    f"Éligibles journées bien-être : "
    f"{delta_wellbeing_eligible}"
)
print(
    f"Journées attribuées          : "
    f"{delta_total_wellbeing_days}"
)


# ============================================================
# 17. VERDICT DELTA
# ============================================================

delta_checks = [
    delta_rows == EXPECTED_EMPLOYEES,
    delta_distinct_employees == EXPECTED_EMPLOYEES,
    delta_duplicates == 0,

    delta_bonus_eligible == EXPECTED_BONUS_ELIGIBLE,

    abs(
        float(delta_total_bonus)
        - EXPECTED_TOTAL_BONUS
    ) < 0.01,

    delta_wellbeing_eligible
    == EXPECTED_WELLBEING_ELIGIBLE,

    delta_total_wellbeing_days
    == EXPECTED_WELLBEING_DAYS
]


print()
print("=" * 60)
print("VERDICT DELTA")
print("=" * 60)

if all(delta_checks):

    print("ÉCRITURE DELTA OK.")
    print(
        "employee_benefits est correctement "
        "enregistrée dans Delta Lake."
    )

else:

    print("ÉCRITURE DELTA KO.")
    print(
        "Les valeurs relues depuis Delta ne correspondent "
        "pas aux valeurs de référence."
    )

# ============================================================
# FIN
# ============================================================

spark.stop()