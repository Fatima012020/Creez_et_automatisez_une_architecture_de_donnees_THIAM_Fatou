"""
02_parse_activities.py

Étape 8.5 - Sport Data Solution

Objectif :
    Lire les événements Debezium provenant de Redpanda,
    extraire uniquement payload.after et transformer
    l'événement CDC en données d'activité exploitables.

Pipeline :

    PostgreSQL
        ↓
    Debezium
        ↓
    Redpanda
        ↓
    Spark Structured Streaming
        ↓
    Parsing JSON
        ↓
    payload.after
        ↓
    Activité propre affichée dans la console

À cette étape :
    - aucune écriture Delta Lake ;
    - aucun calcul métier ;
    - aucun employee_benefits.
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


# ============================================================
# CRÉATION DE LA SESSION SPARK
# ============================================================

spark = (
    SparkSession.builder
    .appName("SportData-Parse-Activities")
    .getOrCreate()
)

spark.sparkContext.setLogLevel("WARN")


# ============================================================
# SCHÉMA DE L'ACTIVITÉ CONTENUE DANS payload.after
# ============================================================
#
# Debezium encapsule les données PostgreSQL dans une enveloppe.
#
# Nous nous intéressons uniquement à :
#
# payload
#    └── after
#          ├── activity_id
#          ├── employee_id
#          ├── start_datetime
#          ├── sport_type
#          ├── distance_meters
#          ├── end_datetime
#          └── comment
#
# start_datetime et end_datetime sont transmis par Debezium
# sous forme de MicroTimestamp, donc en microsecondes.
#
# distance_meters est un DECIMAL PostgreSQL. Avec la
# configuration Debezium actuelle, il est sérialisé dans
# le JSON sous forme de chaîne/base64 lorsqu'il n'est pas NULL.
# Pour l'étape 8.5, nous le conservons donc temporairement
# comme StringType. Sa conversion numérique sera traitée
# proprement avant l'écriture Delta.


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
# SCHÉMA DU PAYLOAD DEBEZIUM
# ============================================================
#
# Nous n'avons pas besoin de reproduire tout le gigantesque
# schéma Debezium.
#
# Spark peut ne parser que les champs dont nous avons besoin :
#
# payload.after
# payload.op
#
# op nous permet de connaître le type d'événement CDC :
#
# c = create
# u = update
# d = delete
# r = snapshot/read

payload_schema = StructType([
    StructField("after", activity_schema, True),
    StructField("op", StringType(), True),
])

debezium_schema = StructType([
    StructField("payload", payload_schema, True),
])


# ============================================================
# LECTURE REDPANDA
# ============================================================

raw_stream = (
    spark.readStream
    .format("kafka")
    .option(
        "kafka.bootstrap.servers",
        REDPANDA_BOOTSTRAP_SERVERS
    )
    .option("subscribe", TOPIC_NAME)

    # Nous voulons uniquement les événements créés
    # après le lancement de ce test.
    .option("startingOffsets", "latest")

    .load()
)


# ============================================================
# CONVERSION DU MESSAGE KAFKA EN JSON
# ============================================================
#
# Kafka/Redpanda fournit "value" en binaire.
#
# Première étape :
#
# binary
#    ↓
# string
#    ↓
# JSON structuré Spark

json_stream = raw_stream.select(
    col("value").cast("string").alias("json_value")
)

parsed_stream = json_stream.select(
    from_json(
        col("json_value"),
        debezium_schema
    ).alias("debezium")
)


# ============================================================
# EXTRACTION DE payload.after
# ============================================================
#
# On extrait :
#
# debezium.payload.after
#
# On conserve également "op" pour pouvoir filtrer
# les types d'événements.

after_stream = parsed_stream.select(
    col("debezium.payload.after.*"),
    col("debezium.payload.op").alias("operation")
)


# ============================================================
# FILTRAGE DES ÉVÉNEMENTS
# ============================================================
#
# Pour notre test actuel, nous voulons les INSERT PostgreSQL.
#
# Debezium les représente par :
#
# op = "c"
#
# On élimine également les événements qui n'ont pas de
# payload.after exploitable.

created_activities = after_stream.filter(
    (col("operation") == "c")
    & col("activity_id").isNotNull()
)


# ============================================================
# CONVERSION DES TIMESTAMPS DEBEZIUM
# ============================================================
#
# Debezium nous transmet ici des MicroTimestamp :
#
# exemple :
#
# 1790524311747227
#
# Cela représente un nombre de MICROSECONDES depuis Unix Epoch.
#
# Spark/from_unixtime attend des secondes.
#
# On divise donc par :
#
# 1 000 000
#
# avant de convertir le résultat en timestamp.

clean_activities = created_activities.select(

    col("activity_id"),

    col("employee_id"),

    (
        from_unixtime(
            col("start_datetime") / 1_000_000
        )
        .cast("timestamp")
        .alias("start_datetime")
    ),

    col("sport_type"),

    # Conservé temporairement tel que reçu par Debezium.
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
# AFFICHAGE
# ============================================================

print("=" * 60)
print("SPARK - PARSING DES ACTIVITÉS DEBEZIUM")
print("=" * 60)
print(f"Broker : {REDPANDA_BOOTSTRAP_SERVERS}")
print(f"Topic  : {TOPIC_NAME}")
print()
print("En attente d'une nouvelle activité...")
print("=" * 60)


query = (
    clean_activities.writeStream
    .format("console")
    .outputMode("append")
    .option("truncate", "false")
    .start()
)


# ============================================================
# MAINTIEN DU STREAM
# ============================================================

try:
    query.awaitTermination()

except KeyboardInterrupt:
    print()
    print("Arrêt demandé par l'utilisateur.")

finally:
    query.stop()
    spark.stop()

    print("Spark Structured Streaming arrêté.")