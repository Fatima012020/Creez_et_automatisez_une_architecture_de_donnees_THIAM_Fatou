"""
01_read_redpanda.py

Étape 8.4 du projet Sport Data Solution.

Objectif :
    Lire avec Spark Structured Streaming les événements CDC produits
    par Debezium et stockés dans le topic Redpanda :

        sport-data.public.activities

À cette étape :
    - aucune transformation métier ;
    - aucune écriture Delta Lake ;
    - aucun calcul employee_benefits.

On vérifie uniquement le flux :

    PostgreSQL
        -> Debezium
        -> Redpanda
        -> Spark Structured Streaming
        -> Console
"""

from pyspark.sql import SparkSession
from pyspark.sql.functions import col


# -------------------------------------------------------------------
# CONFIGURATION
# -------------------------------------------------------------------

# Adresse de Redpanda vue depuis le réseau Docker.
#
# Spark est lui aussi exécuté dans Docker.
# Il doit donc utiliser le nom du service Docker "redpanda"
# et le port Kafka interne 9092.
#
# On NE doit PAS utiliser localhost:19092 ici.
REDPANDA_BOOTSTRAP_SERVERS = "redpanda:9092"

# Topic créé automatiquement par Debezium pour la table activities.
TOPIC_NAME = "sport-data.public.activities"


# -------------------------------------------------------------------
# CRÉATION DE LA SESSION SPARK
# -------------------------------------------------------------------

spark = (
    SparkSession.builder
    .appName("SportData-Read-Redpanda")
    .getOrCreate()
)

# On réduit les logs Spark pour rendre le terminal plus lisible.
spark.sparkContext.setLogLevel("WARN")


print("=" * 60)
print("SPARK STRUCTURED STREAMING - REDPANDA")
print("=" * 60)
print(f"Broker Redpanda : {REDPANDA_BOOTSTRAP_SERVERS}")
print(f"Topic écouté    : {TOPIC_NAME}")
print()


# -------------------------------------------------------------------
# LECTURE DU TOPIC REDPANDA
# -------------------------------------------------------------------
#
# Redpanda étant compatible avec le protocole Kafka,
# Spark utilise son connecteur Kafka standard.
#
# readStream :
#     indique que nous créons un flux continu.
#
# format("kafka") :
#     utilise le connecteur Spark/Kafka.
#
# kafka.bootstrap.servers :
#     adresse du broker Redpanda.
#
# subscribe :
#     topic que Spark doit écouter.
#
# startingOffsets = latest :
#     Spark commence à partir des nouveaux événements.
#
# C'est volontaire pour notre test :
# nous voulons démarrer Spark, puis créer UNE nouvelle activité
# dans PostgreSQL et vérifier qu'elle arrive jusqu'ici.

raw_stream = (
    spark.readStream
    .format("kafka")
    .option("kafka.bootstrap.servers", REDPANDA_BOOTSTRAP_SERVERS)
    .option("subscribe", TOPIC_NAME)
    .option("startingOffsets", "latest")
    .load()
)


# -------------------------------------------------------------------
# PRÉPARATION DE L'AFFICHAGE
# -------------------------------------------------------------------
#
# Le connecteur Kafka fournit notamment :
#
#   key
#   value
#   topic
#   partition
#   offset
#   timestamp
#
# key et value arrivent sous forme binaire.
# Pour pouvoir lire le JSON produit par Debezium dans le terminal,
# nous convertissons ces deux champs en chaînes de caractères.
#
# À cette étape, nous ne décodons PAS encore payload.after.
# Cela sera fait à l'étape suivante.

console_stream = raw_stream.select(
    col("key").cast("string").alias("key"),
    col("value").cast("string").alias("value"),
    col("topic"),
    col("partition"),
    col("offset"),
    col("timestamp"),
)


# -------------------------------------------------------------------
# SORTIE CONSOLE
# -------------------------------------------------------------------
#
# Spark affichera les événements reçus directement dans le terminal.
#
# outputMode("append") :
#     affiche uniquement les nouvelles lignes reçues.
#
# truncate=False :
#     évite que Spark coupe le JSON Debezium dans l'affichage.

query = (
    console_stream.writeStream
    .format("console")
    .outputMode("append")
    .option("truncate", "false")
    .start()
)


print("Spark est démarré.")
print("En attente d'une nouvelle activité...")
print()
print("Laisse ce terminal ouvert puis crée une activité")
print("dans PostgreSQL depuis un deuxième terminal.")
print("=" * 60)


# -------------------------------------------------------------------
# MAINTIEN DU STREAMING
# -------------------------------------------------------------------
#
# Sans awaitTermination(), le programme Python se terminerait
# immédiatement.
#
# Cette instruction maintient Spark actif afin qu'il continue
# d'écouter Redpanda.

try:
    query.awaitTermination()

except KeyboardInterrupt:
    print()
    print("Arrêt demandé par l'utilisateur.")

finally:
    query.stop()
    spark.stop()

    print("Spark Structured Streaming arrêté.")