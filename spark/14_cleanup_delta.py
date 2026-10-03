from pyspark.sql import SparkSession
from delta.tables import DeltaTable

# Chemin de la table Delta des activités
DELTA_PATH = "/opt/spark/work-dir/delta/activities"

# Création de la session Spark avec Delta Lake
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

# Ouverture de la table Delta
delta_table = DeltaTable.forPath(spark, DELTA_PATH)

print("=" * 60)
print("NETTOYAGE DELTA")
print("=" * 60)

# État avant nettoyage
before = spark.read.format("delta").load(DELTA_PATH)

print(f"Lignes avant nettoyage : {before.count()}")

print("\nActivités avec activity_id > 2550 :")

before.filter(
    "activity_id > 2550"
).orderBy(
    "activity_id"
).show(truncate=False)

# Suppression uniquement des activités de démonstration
delta_table.delete("activity_id > 2550")

# Vérification après suppression
after = spark.read.format("delta").load(DELTA_PATH)

print("=" * 60)
print("RÉSULTAT")
print("=" * 60)

print(f"Lignes après nettoyage : {after.count()}")

max_id = after.agg(
    {"activity_id": "max"}
).first()[0]

print(f"Dernier activity_id : {max_id}")

if after.count() == 2550 and max_id == 2550:
    print("\nNETTOYAGE DELTA : OK")
else:
    print("\nNETTOYAGE DELTA : À VÉRIFIER")

spark.stop()