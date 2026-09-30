"""
07_merge_postgres_to_delta.py

Étape 8.8.2 - Sport Data Solution

Objectif :
    Initialiser la table Delta Lake "activities" avec l'historique
    complet présent dans PostgreSQL, sans créer de doublons.

Principe :

    PostgreSQL public.activities
              ↓ JDBC
            Spark
              ↓
        Delta Lake MERGE
              ↓
      data/delta/activities

Clé du MERGE :
    activity_id

Règle :
    - activity_id existe déjà dans Delta -> UPDATE
    - activity_id absent de Delta        -> INSERT

Ce script est rejouable :
le relancer ne doit pas multiplier les lignes.
"""

import os

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, count, countDistinct
from delta.tables import DeltaTable


# ============================================================
# CONFIGURATION
# ============================================================

POSTGRES_HOST = "postgres"
POSTGRES_PORT = "5432"
POSTGRES_DB = "sport_data_db"
POSTGRES_USER = "sport_user"

# Option simple pour notre POC :
# renseigner la même valeur que POSTGRES_PASSWORD dans .env.
#
# Ne mets évidemment pas ton mot de passe dans Git.
# Le mot de passe PostgreSQL est fourni par une variable
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

TABLE_NAME = "public.activities"

DELTA_PATH = "/opt/spark/work-dir/delta/activities"


# ============================================================
# SESSION SPARK + DELTA
# ============================================================

spark = (
    SparkSession.builder
    .appName("SportData-Merge-Postgres-To-Delta")
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
print("INITIALISATION HISTORIQUE POSTGRESQL -> DELTA")
print("=" * 60)

print(f"Source      : {TABLE_NAME}")
print(f"Destination : {DELTA_PATH}")
print("Clé MERGE   : activity_id")
print()


# ============================================================
# 1. LECTURE DE POSTGRESQL
# ============================================================

print("Lecture de PostgreSQL...")

postgres_df = (
    spark.read
    .format("jdbc")
    .option("url", JDBC_URL)
    .option("dbtable", TABLE_NAME)
    .option("user", POSTGRES_USER)
    .option("password", POSTGRES_PASSWORD)
    .option("driver", "org.postgresql.Driver")
    .load()
)

# ============================================================
# HARMONISATION DU SCHÉMA AVEC DELTA LAKE
# ============================================================
#
# PostgreSQL expose distance_meters en DECIMAL(10,2),
# tandis que la table Delta utilise DOUBLE.
#
# On convertit explicitement la colonne avant le MERGE afin
# que les schémas source et destination soient compatibles.

postgres_df = postgres_df.withColumn(
    "distance_meters",
    col("distance_meters").cast("double")
)

# ============================================================
# 2. CONTRÔLE DE LA SOURCE
# ============================================================

source_stats = (
    postgres_df
    .agg(
        count("*").alias("total"),
        countDistinct("activity_id").alias("distinct_ids")
    )
    .first()
)

source_total = source_stats["total"]
source_distinct = source_stats["distinct_ids"]

print(f"Activités PostgreSQL       : {source_total}")
print(f"Activity ID distincts      : {source_distinct}")

if source_total != source_distinct:
    raise ValueError(
        "La source PostgreSQL contient des doublons sur activity_id. "
        "MERGE annulé."
    )

print("Contrôle source            : OK")


# ============================================================
# 3. CONTRÔLE DE LA TABLE DELTA AVANT MERGE
# ============================================================

if not DeltaTable.isDeltaTable(spark, DELTA_PATH):
    raise RuntimeError(
        f"Aucune table Delta valide trouvée dans {DELTA_PATH}. "
        "L'étape 8.7 doit être validée avant d'exécuter ce script."
    )


delta_before_df = (
    spark.read
    .format("delta")
    .load(DELTA_PATH)
)

delta_before_count = delta_before_df.count()

print()
print(f"Activités Delta avant MERGE : {delta_before_count}")


# ============================================================
# 4. MERGE POSTGRESQL -> DELTA
# ============================================================
#
# activity_id est notre clé.
#
# Cas 1 :
#     l'activité existe déjà
#         -> UPDATE de toutes ses colonnes
#
# Cas 2 :
#     l'activité n'existe pas
#         -> INSERT
#
# Cela rend l'initialisation rejouable.

print()
print("MERGE PostgreSQL -> Delta en cours...")


delta_table = DeltaTable.forPath(
    spark,
    DELTA_PATH
)


(
    delta_table.alias("target")
    .merge(
        postgres_df.alias("source"),
        "target.activity_id = source.activity_id"
    )
    .whenMatchedUpdateAll()
    .whenNotMatchedInsertAll()
    .execute()
)


print("MERGE terminé.")


# ============================================================
# 5. RELECTURE DE DELTA APRÈS MERGE
# ============================================================

delta_after_df = (
    spark.read
    .format("delta")
    .load(DELTA_PATH)
)


after_stats = (
    delta_after_df
    .agg(
        count("*").alias("total"),
        countDistinct("activity_id").alias("distinct_ids")
    )
    .first()
)

delta_total = after_stats["total"]
delta_distinct = after_stats["distinct_ids"]
duplicates = delta_total - delta_distinct


# ============================================================
# 6. RÉSUMÉ
# ============================================================

print()
print("=" * 60)
print("RÉSUMÉ DU MERGE")
print("=" * 60)

print(f"PostgreSQL                  : {source_total}")
print(f"Delta avant MERGE           : {delta_before_count}")
print(f"Delta après MERGE           : {delta_total}")
print(f"Activity ID distincts       : {delta_distinct}")
print(f"Doublons sur activity_id    : {duplicates}")


# ============================================================
# 7. CONTRÔLE DE L'ACTIVITÉ 2549
# ============================================================
#
# L'activité 2549 est déjà présente dans Delta grâce au
# streaming. Le MERGE ne doit donc pas la dupliquer.

print()
print("=" * 60)
print("CONTRÔLE ACTIVITY_ID 2549")
print("=" * 60)

(
    delta_after_df
    .filter("activity_id = 2549")
    .show(truncate=False)
)

# ============================================================
# 8. VERDICT
# ============================================================

print()
print("=" * 60)
print("VERDICT")
print("=" * 60)

if duplicates != 0:

    print("MERGE KO : doublons détectés.")

elif delta_total != source_total:

    print(
        "ATTENTION : le nombre de lignes Delta "
        "diffère du nombre de lignes PostgreSQL."
    )

else:

    print("MERGE OK.")
    print("Historique PostgreSQL correctement initialisé dans Delta.")
    print("Aucun doublon sur activity_id.")


spark.stop()