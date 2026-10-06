from pyspark.sql import SparkSession
from delta.tables import DeltaTable

# ============================================================
# CONFIGURATION
# ============================================================

DELTA_PATH = "/opt/spark/work-dir/delta/activities"

# Activités de test/démonstration à supprimer.
# L'objectif est de revenir à l'état de référence :
# 2 549 activités, avec activity_id = 2549 comme dernier ID.
ACTIVITY_IDS_TO_DELETE = [
    2550,
    2559,
    2560,
    2561,
    2562,
    2563,
    2564,
    2597,
    2598,
    2599,
    2600,
]


# ============================================================
# SESSION SPARK + DELTA LAKE
# ============================================================

spark = (
    SparkSession.builder
    .appName("CleanupDeltaActivities")
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


# ============================================================
# OUVERTURE DE LA TABLE DELTA
# ============================================================

delta_table = DeltaTable.forPath(spark, DELTA_PATH)

before = spark.read.format("delta").load(DELTA_PATH)

print("=" * 60)
print("NETTOYAGE DELTA")
print("=" * 60)

before_count = before.count()

print(f"Lignes avant nettoyage : {before_count}")


# ============================================================
# AFFICHAGE DES ACTIVITÉS QUI SERONT SUPPRIMÉES
# ============================================================

ids_string = ",".join(str(activity_id) for activity_id in ACTIVITY_IDS_TO_DELETE)

condition = f"activity_id IN ({ids_string})"

print("\nActivités qui vont être supprimées :")

before.filter(condition).orderBy("activity_id").show(
    truncate=False
)


# ============================================================
# SUPPRESSION
# ============================================================

delta_table.delete(condition)


# ============================================================
# VÉRIFICATION
# ============================================================

after = spark.read.format("delta").load(DELTA_PATH)

after_count = after.count()

max_id = after.agg(
    {"activity_id": "max"}
).first()[0]

print("=" * 60)
print("RÉSULTAT")
print("=" * 60)

print(f"Lignes après nettoyage : {after_count}")
print(f"Dernier activity_id     : {max_id}")


# ============================================================
# VALIDATION DE L'ÉTAT DE RÉFÉRENCE
# ============================================================

if after_count == 2549 and max_id == 2549:
    print("\nNETTOYAGE DELTA : OK")
    print("Delta Lake est prêt pour la démonstration.")
else:
    print("\nNETTOYAGE DELTA : À VÉRIFIER")
    print("État attendu : 2549 lignes et dernier activity_id = 2549.")


spark.stop()