"""
05_read_delta_activities.py

Étape 8.7.3 - Sport Data Solution

Objectif :
    Vérifier les données réellement enregistrées dans
    la table Delta Lake "activities".

Le script :
    - lit la table Delta ;
    - affiche son schéma ;
    - compte les lignes ;
    - affiche les activités stockées.

Aucune modification des données n'est réalisée.
"""

from pyspark.sql import SparkSession
from pyspark.sql.functions import col


# ============================================================
# CONFIGURATION
# ============================================================

DELTA_PATH = "/opt/spark/work-dir/delta/activities"


# ============================================================
# SESSION SPARK + DELTA
# ============================================================

spark = (
    SparkSession.builder
    .appName("SportData-Read-Delta-Activities")
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
print("LECTURE DELTA LAKE - ACTIVITIES")
print("=" * 60)
print(f"Source : {DELTA_PATH}")
print()


# ============================================================
# LECTURE DE LA TABLE DELTA
# ============================================================

activities_df = (
    spark.read
    .format("delta")
    .load(DELTA_PATH)
)


# ============================================================
# SCHÉMA
# ============================================================

print("=" * 60)
print("SCHÉMA")
print("=" * 60)

activities_df.printSchema()


# ============================================================
# COMPTAGE
# ============================================================

row_count = activities_df.count()

print()
print("=" * 60)
print("RÉSUMÉ")
print("=" * 60)
print(f"Nombre d'activités dans Delta : {row_count}")


# ============================================================
# AFFICHAGE DES ACTIVITÉS
# ============================================================

print()
print("=" * 60)
print("ACTIVITÉS STOCKÉES")
print("=" * 60)

(
    activities_df
    .orderBy(col("activity_id"))
    .show(
        n=100,
        truncate=False
    )
)


# ============================================================
# CONTRÔLE DE LA DERNIÈRE ACTIVITÉ
# ============================================================

print()
print("=" * 60)
print("DERNIÈRE ACTIVITÉ")
print("=" * 60)

(
    activities_df
    .orderBy(col("activity_id").desc())
    .show(
        n=1,
        truncate=False
    )
)


print()
print("Lecture Delta terminée avec succès.")

spark.stop()