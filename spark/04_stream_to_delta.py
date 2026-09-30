"""
04_stream_to_delta.py

Étape 8.7.2 - Sport Data Solution

Objectif :
    Consommer en continu les événements CDC de la table activities
    depuis Redpanda avec Spark Structured Streaming, nettoyer
    l'enveloppe Debezium et écrire les activités dans Delta Lake.

Pipeline :

    PostgreSQL
        ↓
    Debezium
        ↓
    Redpanda
        ↓
    Spark Structured Streaming
        ↓
    Parsing payload.after
        ↓
    Conversion des types
        ↓
    Delta Lake

Sortie :
    /opt/spark/work-dir/delta/activities

Checkpoint :
    /opt/spark/work-dir/checkpoints/activities
"""


from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col,
    from_json,
    from_unixtime,
)
from pyspark.sql.types import (
    StructType,
    StructField,
    StringType,
    IntegerType,
    LongType,
    DoubleType,
)


# ============================================================
# CONFIGURATION
# ============================================================

REDPANDA_BOOTSTRAP_SERVERS = "redpanda:9092"

TOPIC_NAME = "sport-data.public.activities"

DELTA_PATH = "/opt/spark/work-dir/delta/activities"

CHECKPOINT_PATH = (
    "/opt/spark/work-dir/checkpoints/activities"
)


# ============================================================
# SESSION SPARK + DELTA LAKE
# ============================================================

spark = (
    SparkSession.builder
    .appName("SportData-Activities-To-Delta")
    .config(
        "spark.sql.extensions",
        "io.delta.sql.DeltaSparkSessionExtension"
    )
    .config(
        "spark.sql.catalog.spark_catalog",
        "org.apache.spark.sql.delta.catalog.DeltaCatalog"
    )
    .config(
        "spark.sql.adaptive.enabled",
        "false"
    )
    .getOrCreate()
)

spark.sparkContext.setLogLevel("WARN")
# Masque les avertissements internes Kafka sans masquer
# les autres avertissements utiles de Spark.
spark._jvm.org.apache.log4j.LogManager.getLogger(
    "org.apache.kafka.clients.admin.AdminClientConfig"
).setLevel(
    spark._jvm.org.apache.log4j.Level.ERROR
)

# ============================================================
# SCHÉMA DE payload.after
# ============================================================
#
# distance_meters reste ici temporairement StringType
# parce que Debezium sérialise le DECIMAL PostgreSQL
# en bytes encodés en Base64.
#
# Nous avons observé :
#
# PostgreSQL : 5000.00
# Debezium   : B6Eg
#
# La conversion est effectuée plus bas.

activity_schema = StructType([
    StructField("activity_id", IntegerType(), True),
    StructField("employee_id", IntegerType(), True),
    StructField("start_datetime", LongType(), True),
    StructField("sport_type", StringType(), True),
    StructField("distance_meters", DoubleType(), True),
    StructField("end_datetime", LongType(), True),
    StructField("comment", StringType(), True),
])


# ============================================================
# SCHÉMA MINIMAL DEBEZIUM
# ============================================================
#
# Nous ne parsont volontairement que les informations
# nécessaires :
#
# payload.after
# payload.op

payload_schema = StructType([
    StructField("after", activity_schema, True),
    StructField("op", StringType(), True),
])

debezium_schema = StructType([
    StructField("payload", payload_schema, True),
])

# ============================================================
# LECTURE DU TOPIC REDPANDA
# ============================================================
#
# IMPORTANT :
#
# Spark est dans Docker.
# Redpanda est dans Docker.
#
# L'adresse est donc :
#
# redpanda:9092
#
# et non localhost:19092.

raw_stream = (
    spark.readStream
    .format("kafka")
    .option(
        "kafka.bootstrap.servers",
        REDPANDA_BOOTSTRAP_SERVERS
    )
    .option(
        "subscribe",
        TOPIC_NAME
    )

    # Pour cette première création de la table Delta,
    # on part uniquement des nouveaux événements.
    #
    # Nous testerons donc :
    #
    # démarrage Spark
    #       ↓
    # INSERT PostgreSQL
    #       ↓
    # nouvelle activité Delta
    .option(
        "startingOffsets",
        "latest"
    )

    .load()
)


# ============================================================
# BINARY → STRING → JSON DEBEZIUM
# ============================================================

json_stream = raw_stream.select(
    col("value")
    .cast("string")
    .alias("json_value")
)


parsed_stream = json_stream.select(
    from_json(
        col("json_value"),
        debezium_schema
    ).alias("debezium")
)


# ============================================================
# EXTRACTION payload.after
# ============================================================

after_stream = parsed_stream.select(
    col("debezium.payload.after.*"),
    col("debezium.payload.op").alias("operation")
)


# ============================================================
# FILTRAGE DES INSERT
# ============================================================
#
# Pour cette première version minimale :
#
# c = CREATE
#
# Nous écrivons uniquement les nouvelles activités.
#
# Les UPDATE/DELETE seront volontairement laissés de côté
# pour l'instant.

created_activities = after_stream.filter(
    (col("operation") == "c")
    & col("activity_id").isNotNull()
)


# ============================================================
# NETTOYAGE ET CONVERSION DES TYPES
# ============================================================

clean_activities = created_activities.select(

    col("activity_id"),

    col("employee_id"),

    # MicroTimestamp Debezium
    # → secondes Unix
    # → Timestamp Spark
    (
        from_unixtime(
            col("start_datetime") / 1_000_000
        )
        .cast("timestamp")
        .alias("start_datetime")
    ),

    col("sport_type"),

    col("distance_meters"),

    (
        from_unixtime(
            col("end_datetime") / 1_000_000
        )
        .cast("timestamp")
        .alias("end_datetime")
    ),

    col("comment")
)


# ============================================================
# AFFICHAGE DE LA CONFIGURATION
# ============================================================

print("=" * 60)
print("REDPANDA -> SPARK -> DELTA LAKE")
print("=" * 60)

print(f"Broker     : {REDPANDA_BOOTSTRAP_SERVERS}")
print(f"Topic      : {TOPIC_NAME}")
print(f"Delta      : {DELTA_PATH}")
print(f"Checkpoint : {CHECKPOINT_PATH}")

print()
print("En attente de nouvelles activités...")
print("=" * 60)


# ============================================================
# ÉCRITURE STREAMING VERS DELTA LAKE
# ============================================================
#
# format("delta")
#     indique que la destination est une table Delta.
#
# outputMode("append")
#     ajoute uniquement les nouvelles activités.
#
# checkpointLocation
#     mémorise la progression du streaming.
#
# Spark pourra ainsi retrouver les offsets Redpanda déjà
# traités lors d'un redémarrage.

query = (
    clean_activities.writeStream
    .format("delta")
    .outputMode("append")
    .option(
        "checkpointLocation",
        CHECKPOINT_PATH
    )
    .start(
        DELTA_PATH
    )
)


# ============================================================
# MAINTIEN DU STREAMING
# ============================================================

try:

    query.awaitTermination()

except KeyboardInterrupt:

    print()
    print("Arrêt demandé par l'utilisateur.")

finally:

    query.stop()
    spark.stop()

    print("Streaming Redpanda -> Delta arrêté.")