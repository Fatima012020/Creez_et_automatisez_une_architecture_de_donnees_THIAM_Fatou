"""
03_test_delta.py

Étape 8.7.1 - Sport Data Solution

Objectif :
    Vérifier que notre environnement Spark 3.5.3
    peut utiliser Delta Lake 3.3.2.

Ce script :
    1. crée un petit DataFrame Spark ;
    2. l'écrit au format Delta ;
    3. relit la table Delta ;
    4. affiche son contenu.

Aucune donnée Redpanda n'est utilisée à cette étape.
"""

from pyspark.sql import SparkSession


# ============================================================
# EMPLACEMENT DE LA TABLE DELTA DE TEST
# ============================================================
#
# Ce chemin correspond au volume Docker :
#
# Windows :
#   ./data/delta
#
# Docker :
#   /opt/spark/work-dir/delta
#
# La table de test sera donc physiquement enregistrée dans :
#
#   data/delta/test_delta

DELTA_PATH = "/opt/spark/work-dir/delta/test_delta"


# ============================================================
# CRÉATION DE LA SESSION SPARK
# ============================================================
#
# Les extensions Delta sont également passées à spark-submit.
# On les configure explicitement afin que Spark utilise
# le catalogue Delta.

spark = (
    SparkSession.builder
    .appName("SportData-Test-Delta")
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
print("TEST DELTA LAKE")
print("=" * 60)
print(f"Destination : {DELTA_PATH}")


# ============================================================
# CRÉATION D'UN PETIT DATAFRAME
# ============================================================

data = [
    (1, "Running", 5000.00),
    (2, "Tennis", None),
    (3, "Randonnée", 8000.00),
]

columns = [
    "test_id",
    "sport_type",
    "distance_meters",
]

df = spark.createDataFrame(data, columns)


print()
print("DataFrame Spark à enregistrer :")
df.show(truncate=False)


# ============================================================
# ÉCRITURE DELTA
# ============================================================
#
# mode("overwrite") est volontaire ici :
# il s'agit uniquement d'une table de test rejouable.

print()
print("Écriture de la table Delta...")

(
    df.write
    .format("delta")
    .mode("overwrite")
    .save(DELTA_PATH)
)

print("Écriture Delta réussie.")


# ============================================================
# LECTURE DELTA
# ============================================================

print()
print("Relecture de la table Delta...")

delta_df = (
    spark.read
    .format("delta")
    .load(DELTA_PATH)
)

delta_df.show(truncate=False)


# ============================================================
# CONTRÔLE
# ============================================================

row_count = delta_df.count()

print(f"Nombre de lignes relues : {row_count}")

if row_count == 3:
    print()
    print("TEST DELTA LAKE RÉUSSI.")
else:
    raise RuntimeError(
        f"Nombre de lignes inattendu : {row_count}"
    )


spark.stop()