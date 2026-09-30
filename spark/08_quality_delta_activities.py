"""
08_quality_delta_activities.py

Étape 8.8.3 - Sport Data Solution

Objectif :
    Effectuer le contrôle qualité final de la table Delta
    "activities" avant son utilisation pour le calcul métier
    employee_benefits.

Contrôles :
    - unicité de activity_id ;
    - activity_id NULL ;
    - employee_id NULL ;
    - sport_type NULL ou vide ;
    - dates NULL ;
    - end_datetime antérieur à start_datetime ;
    - distances négatives ;
    - statistiques générales.

Ce script est strictement en lecture seule.
Il ne modifie aucune donnée.
"""

from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col,
    count,
    countDistinct,
    trim,
    min as spark_min,
    max as spark_max,
)


# ============================================================
# CONFIGURATION
# ============================================================

DELTA_PATH = "/opt/spark/work-dir/delta/activities"


# ============================================================
# SESSION SPARK + DELTA
# ============================================================

spark = (
    SparkSession.builder
    .appName("SportData-Quality-Delta-Activities")
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
print("CONTRÔLE QUALITÉ - DELTA ACTIVITIES")
print("=" * 60)
print(f"Source : {DELTA_PATH}")


# ============================================================
# LECTURE DELTA
# ============================================================

activities_df = (
    spark.read
    .format("delta")
    .load(DELTA_PATH)
)


# ============================================================
# STATISTIQUES GÉNÉRALES
# ============================================================

stats = (
    activities_df
    .agg(
        count("*").alias("total"),
        countDistinct("activity_id").alias("distinct_activity_ids"),
        countDistinct("employee_id").alias("distinct_employees"),
        countDistinct("sport_type").alias("distinct_sports"),
        spark_min("start_datetime").alias("first_activity"),
        spark_max("start_datetime").alias("last_activity"),
    )
    .first()
)


total = stats["total"]
distinct_ids = stats["distinct_activity_ids"]
employees = stats["distinct_employees"]
sports = stats["distinct_sports"]

duplicates = total - distinct_ids


print()
print("=" * 60)
print("RÉSUMÉ GÉNÉRAL")
print("=" * 60)

print(f"Activités                : {total}")
print(f"Activity ID distincts    : {distinct_ids}")
print(f"Salariés représentés     : {employees}")
print(f"Sports différents        : {sports}")
print(f"Première activité        : {stats['first_activity']}")
print(f"Dernière activité        : {stats['last_activity']}")


# ============================================================
# CONTRÔLES QUALITÉ
# ============================================================

null_activity_id = (
    activities_df
    .filter(col("activity_id").isNull())
    .count()
)

null_employee_id = (
    activities_df
    .filter(col("employee_id").isNull())
    .count()
)

invalid_sport = (
    activities_df
    .filter(
        col("sport_type").isNull()
        | (trim(col("sport_type")) == "")
    )
    .count()
)

null_start_datetime = (
    activities_df
    .filter(col("start_datetime").isNull())
    .count()
)

null_end_datetime = (
    activities_df
    .filter(col("end_datetime").isNull())
    .count()
)

invalid_dates = (
    activities_df
    .filter(
        col("end_datetime") < col("start_datetime")
    )
    .count()
)

negative_distances = (
    activities_df
    .filter(
        col("distance_meters") < 0
    )
    .count()
)


print()
print("=" * 60)
print("CONTRÔLES QUALITÉ")
print("=" * 60)

print(f"Doublons activity_id     : {duplicates}")
print(f"activity_id NULL         : {null_activity_id}")
print(f"employee_id NULL         : {null_employee_id}")
print(f"Sport NULL ou vide       : {invalid_sport}")
print(f"start_datetime NULL      : {null_start_datetime}")
print(f"end_datetime NULL        : {null_end_datetime}")
print(f"Dates incohérentes       : {invalid_dates}")
print(f"Distances négatives      : {negative_distances}")


# ============================================================
# DISTANCES NULL
# ============================================================
#
# Une distance NULL n'est PAS considérée comme une anomalie.
#
# Certains sports (ex. Tennis, Judo, Boxe...) ne nécessitent
# pas nécessairement une distance.
#
# On l'affiche uniquement comme information.

null_distances = (
    activities_df
    .filter(col("distance_meters").isNull())
    .count()
)

print(f"Distances NULL           : {null_distances} (informatif)")


# ============================================================
# RÉPARTITION PAR SPORT
# ============================================================

print()
print("=" * 60)
print("RÉPARTITION PAR SPORT")
print("=" * 60)

(
    activities_df
    .groupBy("sport_type")
    .count()
    .orderBy(col("count").desc())
    .show(
        n=100,
        truncate=False
    )
)


# ============================================================
# VERDICT
# ============================================================

blocking_errors = (
    duplicates
    + null_activity_id
    + null_employee_id
    + invalid_sport
    + null_start_datetime
    + null_end_datetime
    + invalid_dates
    + negative_distances
)


print()
print("=" * 60)
print("VERDICT")
print("=" * 60)

if blocking_errors == 0:

    print("QUALITÉ OK.")
    print("Aucun problème bloquant détecté dans Delta activities.")
    print(
        "La table peut être utilisée pour "
        "le traitement métier suivant."
    )

else:

    print(
        f"QUALITÉ KO : "
        f"{blocking_errors} problème(s) bloquant(s) détecté(s)."
    )


spark.stop()