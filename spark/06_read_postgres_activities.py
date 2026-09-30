"""
06_read_postgres_activities.py

Étape 8.8.1 - Sport Data Solution

Objectif :
    Lire la table PostgreSQL public.activities avec Spark JDBC
    afin de préparer l'initialisation historique de Delta Lake.

IMPORTANT :
    Ce script est en lecture seule.
    Il ne modifie ni PostgreSQL ni Delta Lake.

Pipeline testé :

    PostgreSQL
        ↓ JDBC
      Spark
        ↓
    contrôles
"""
import os
from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col,
    count,
    countDistinct,
)


# ============================================================
# CONFIGURATION POSTGRESQL
# ============================================================
#
# Spark et PostgreSQL sont tous les deux dans Docker.
#
# Spark doit donc joindre PostgreSQL avec :
#
#     postgres:5432
#
# et NON :
#
#     localhost:5433
#
# localhost:5433 est utilisé depuis Windows.

POSTGRES_HOST = "postgres"
POSTGRES_PORT = "5432"
POSTGRES_DB = "sport_data_db"

POSTGRES_USER = "sport_user"

# IMPORTANT :
# remplace uniquement la valeur ci-dessous par le mot de passe
# PostgreSQL présent dans ton fichier .env.
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


# ============================================================
# SESSION SPARK
# ============================================================

spark = (
    SparkSession.builder
    .appName("SportData-Read-Postgres-Activities")
    .getOrCreate()
)

spark.sparkContext.setLogLevel("WARN")


print("=" * 60)
print("LECTURE POSTGRESQL AVEC SPARK JDBC")
print("=" * 60)

print(f"Base  : {POSTGRES_DB}")
print(f"Table : {TABLE_NAME}")

print()


# ============================================================
# LECTURE JDBC
# ============================================================

print("Connexion à PostgreSQL...")

activities_df = (
    spark.read
    .format("jdbc")
    .option("url", JDBC_URL)
    .option("dbtable", TABLE_NAME)
    .option("user", POSTGRES_USER)
    .option("password", POSTGRES_PASSWORD)
    .option("driver", "org.postgresql.Driver")
    .load()
)

print("Connexion PostgreSQL réussie.")


# ============================================================
# SCHÉMA
# ============================================================

print()
print("=" * 60)
print("SCHÉMA POSTGRESQL LU PAR SPARK")
print("=" * 60)

activities_df.printSchema()


# ============================================================
# CONTRÔLES
# ============================================================

stats = (
    activities_df
    .agg(
        count("*").alias("total"),
        countDistinct("activity_id").alias("distinct_ids")
    )
    .first()
)

total = stats["total"]
distinct_ids = stats["distinct_ids"]

duplicates = total - distinct_ids


print()
print("=" * 60)
print("RÉSUMÉ")
print("=" * 60)

print(f"Activités                  : {total}")
print(f"Activity ID distincts      : {distinct_ids}")
print(f"Doublons sur activity_id   : {duplicates}")


# ============================================================
# DERNIÈRES ACTIVITÉS
# ============================================================

print()
print("=" * 60)
print("5 DERNIÈRES ACTIVITÉS")
print("=" * 60)

(
    activities_df
    .select(
        "activity_id",
        "employee_id",
        "start_datetime",
        "sport_type",
        "distance_meters",
        "end_datetime",
        "comment",
    )
    .orderBy(
        col("activity_id").desc()
    )
    .show(
        n=5,
        truncate=False
    )
)


# ============================================================
# CONTRÔLE FINAL
# ============================================================

if duplicates == 0:
    print("Contrôle activity_id : OK")
else:
    print(
        f"ATTENTION : {duplicates} doublon(s) "
        "détecté(s) sur activity_id."
    )


print()
print("Lecture PostgreSQL terminée avec succès.")

spark.stop()