from pyspark.sql import SparkSession
from pyspark.sql.functions import col

DELTA_PATH = "/opt/spark/work-dir/delta/activities"

spark = (
    SparkSession.builder
    .appName("CheckLastDeltaActivity")
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

df = spark.read.format("delta").load(DELTA_PATH)

print("\n" + "=" * 60)
print("DERNIÈRES ACTIVITÉS DELTA")
print("=" * 60)

df.orderBy(col("activity_id").desc()).show(
    5,
    truncate=False
)

print(f"Nombre de lignes Delta : {df.count()}")

spark.stop()