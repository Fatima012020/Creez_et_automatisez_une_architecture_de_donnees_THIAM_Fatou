"""
Consumer Redpanda des activités sportives.

Objectif
--------
Ce script écoute les nouvelles activités capturées par Debezium
et publiées dans Redpanda.

Pour chaque nouvelle activité :
1. le message Debezium est décodé ;
2. seules les créations (INSERT) sont conservées ;
3. le salarié est recherché dans PostgreSQL ;
4. les timestamps Debezium sont convertis ;
5. la durée de l'activité est calculée ;
6. un message destiné à Slack est construit.

À cette étape, le message est uniquement affiché dans le terminal.
L'envoi réel vers Slack sera ajouté ensuite.
"""

import json
from datetime import datetime

import psycopg
from confluent_kafka import Consumer

import os
import requests

from dotenv import load_dotenv

import base64
from decimal import Decimal

# ============================================================
# CONFIGURATION SLACK
# ============================================================

from pathlib import Path

# Racine du projet dans le conteneur Spark
PROJECT_DIR = Path(__file__).resolve().parent.parent

# Chargement explicite du fichier .env
ENV_FILE = PROJECT_DIR / ".env"
load_dotenv(ENV_FILE)

# Le webhook est stocké dans .env afin de ne jamais
# enregistrer ce secret directement dans le code GitHub.
SLACK_WEBHOOK_URL = os.getenv("SLACK_WEBHOOK_URL")

if not SLACK_WEBHOOK_URL:
    raise ValueError(
        "SLACK_WEBHOOK_URL est absente du fichier .env."
    )

# ============================================================
# FONCTION : ENVOI VERS SLACK
# ============================================================

def send_slack_message(message):
    """
    Envoie un message dans le channel Slack configuré
    via l'Incoming Webhook.

    Parameters
    ----------
    message : str
        Message métier construit à partir de l'activité.

    Returns
    -------
    bool
        True si l'envoi réussit, False sinon.
    """

    payload = {
        "text": message
    }

    try:
        response = requests.post(
            SLACK_WEBHOOK_URL,
            json=payload,
            timeout=10,
        )

        response.raise_for_status()

        print("Message envoyé sur Slack avec succès. ✅")

        return True

    except requests.RequestException as error:
        print(f"Erreur lors de l'envoi Slack : {error}")

        return False


# ============================================================
# CONFIGURATION REDPANDA
# ============================================================

# Les valeurs sont récupérées depuis les variables
# d'environnement. Les valeurs par défaut correspondent
# au réseau Docker du projet.
BOOTSTRAP_SERVERS = os.getenv(
    "REDPANDA_BOOTSTRAP_SERVERS",
    "redpanda:9092"
)

TOPIC_NAME = os.getenv(
    "REDPANDA_TOPIC",
    "sport-data.public.activities"
)

GROUP_ID = os.getenv(
    "REDPANDA_GROUP_ID",
    "sport-data-slack-consumer"
)

# ============================================================
# CONFIGURATION POSTGRESQL
# ============================================================

# Ces valeurs doivent correspondre à la configuration
# PostgreSQL utilisée dans le projet.
#
# Plus tard, avant le dépôt GitHub final, ces informations
# sensibles seront déplacées dans les variables d'environnement.

POSTGRES_PASSWORD = os.getenv("POSTGRES_PASSWORD")

if not POSTGRES_PASSWORD:
    raise ValueError(
        "La variable d'environnement POSTGRES_PASSWORD est absente."
    )


DB_CONFIG = {
    "host": os.getenv("POSTGRES_HOST", "postgres"),
    "port": int(os.getenv("POSTGRES_PORT", "5432")),
    "dbname": os.getenv("POSTGRES_DB", "sport_data_db"),
    "user": os.getenv("POSTGRES_USER", "sport_user"),
    "password": POSTGRES_PASSWORD,
}


# ============================================================
# FONCTION : CONVERSION TIMESTAMP DEBEZIUM
# ============================================================

def convert_debezium_timestamp(timestamp_microseconds):
    """
    Convertit un MicroTimestamp Debezium en datetime Python.

    Debezium représente ici les dates PostgreSQL en nombre
    de microsecondes depuis l'époque Unix.

    Exemple :
        1790475303158506
        devient un objet datetime exploitable par Python.

    Parameters
    ----------
    timestamp_microseconds : int
        Timestamp Debezium exprimé en microsecondes.

    Returns
    -------
    datetime | None
        Date convertie, ou None si aucune valeur n'est fournie.
    """

    if timestamp_microseconds is None:
        return None

    # datetime.fromtimestamp() attend des secondes.
    # On convertit donc les microsecondes en secondes.
    timestamp_seconds = timestamp_microseconds / 1_000_000

    return datetime.fromtimestamp(timestamp_seconds)


# ============================================================
# FONCTION : RÉCUPÉRATION DU SALARIÉ
# ============================================================

def get_employee_name(connection, employee_id):
    """
    Recherche le prénom et le nom d'un salarié dans PostgreSQL.

    Parameters
    ----------
    connection : psycopg2 connection
        Connexion active à PostgreSQL.

    employee_id : int
        Identifiant du salarié présent dans l'activité.

    Returns
    -------
    tuple | None
        Tuple (prénom, nom) si le salarié existe,
        sinon None.
    """

    with connection.cursor() as cursor:

        cursor.execute(
            """
            SELECT first_name, last_name
            FROM employees
            WHERE employee_id = %s;
            """,
            (employee_id,),
        )

        return cursor.fetchone()


# ============================================================
# FONCTION : FORMATAGE DE LA DURÉE
# ============================================================

def format_duration(start_datetime, end_datetime):
    """
    Calcule et formate la durée d'une activité.

    Exemple :
        46 minutes  -> "46 min"
        90 minutes  -> "1 h 30 min"
        120 minutes -> "2 h"

    Parameters
    ----------
    start_datetime : datetime
        Date et heure de début.

    end_datetime : datetime
        Date et heure de fin.

    Returns
    -------
    str
        Durée lisible destinée au message Slack.
    """

    if start_datetime is None or end_datetime is None:
        return "durée inconnue"

    duration = end_datetime - start_datetime

    total_minutes = int(duration.total_seconds() // 60)

    hours = total_minutes // 60
    minutes = total_minutes % 60

    if hours == 0:
        return f"{minutes} min"

    if minutes == 0:
        return f"{hours} h"

    return f"{hours} h {minutes} min"


# ============================================================
# FONCTION : FORMATAGE DE LA DISTANCE
# ============================================================

def format_distance(distance_meters):
    """
    Convertit la distance Debezium en kilomètres lisibles.

    Le champ PostgreSQL distance_meters est de type NUMERIC(10, 2).
    Debezium le publie comme un Decimal Kafka Connect :
    - bytes encodés en Base64 ;
    - scale = 2.

    Exemple :
    "CSfA" -> 6000.00 mètres -> 6.00 km
    """

    if distance_meters is None:
        return "Distance non renseignée"

    try:
        # Debezium sérialise le Decimal sous forme Base64.
        decoded_bytes = base64.b64decode(distance_meters)

        # Kafka Connect Decimal utilise un entier signé big-endian.
        unscaled_value = int.from_bytes(
            decoded_bytes,
            byteorder="big",
            signed=True
        )

        # Le schéma Debezium indique scale = 2.
        distance_decimal = Decimal(unscaled_value).scaleb(-2)

        distance_km = distance_decimal / Decimal("1000")

        return f"{distance_km:.2f} km"

    except (ValueError, TypeError):
        return "Distance invalide"
    
# ============================================================
# FONCTION : CONSTRUCTION DU MESSAGE SLACK
# ============================================================

def build_slack_message(
    first_name,
    last_name,
    sport_type,
    duration_text,
    distance_text=None,
    comment=None,
):
    """
    Construit le message métier destiné à Slack.

    Le contenu s'adapte aux informations disponibles :
    - durée toujours affichée si elle est calculable ;
    - distance uniquement lorsqu'elle existe ;
    - commentaire uniquement lorsqu'il est renseigné.

    À cette étape, le message n'est pas encore envoyé à Slack.
    """

    # Début commun à toutes les activités.
    message = (
        f"Bravo {first_name} {last_name} ! "
        f"Tu viens de terminer une activité {sport_type}"
    )

    # Certaines activités possèdent une distance pertinente.
    if distance_text:
        message += f" de {distance_text}"

    # Ajout de la durée.
    message += f" en {duration_text} ! 🔥🏅"

    # Le commentaire est facultatif.
    if comment:
        message += f"\n💬 {comment}"

    return message


# ============================================================
# CONNEXION POSTGRESQL
# ============================================================

print("Connexion à PostgreSQL...")

db_connection = psycopg.connect(**DB_CONFIG)

print("Connexion PostgreSQL réussie.")


# ============================================================
# CRÉATION DU CONSUMER REDPANDA
# ============================================================

consumer = Consumer(
    {
        "bootstrap.servers": BOOTSTRAP_SERVERS,
        "group.id": GROUP_ID,

        # Si aucun offset n'existe encore pour ce groupe,
        # la lecture commence au premier événement disponible.
        "auto.offset.reset": "latest",
    }
)

consumer.subscribe([TOPIC_NAME])


print()
print("=" * 60)
print("CONSUMER REDPANDA - ACTIVITÉS SPORTIVES")
print("=" * 60)
print(f"Topic écouté : {TOPIC_NAME}")
print("En attente d'une activité...")


# ============================================================
# BOUCLE PRINCIPALE
# ============================================================

try:

    while True:

        message = consumer.poll(timeout=1.0)

        if message is None:
            continue

        if message.error():
            print(f"Erreur Redpanda : {message.error()}")
            continue


        # ====================================================
        # DÉCODAGE DE L'ÉVÉNEMENT DEBEZIUM
        # ====================================================

        raw_value = message.value().decode("utf-8")
        event = json.loads(raw_value)

        payload = event.get("payload")

        if payload is None:
            print("Événement ignoré : payload absent.")
            continue


        # Nous publions uniquement les nouvelles activités.
        operation = payload.get("op")

        if operation != "c":
            print(
                f"Événement ignoré : opération Debezium '{operation}'."
            )
            continue


        # "after" correspond à la nouvelle ligne PostgreSQL.
        activity = payload.get("after")

        if activity is None:
            print("Événement ignoré : données 'after' absentes.")
            continue


        # ====================================================
        # EXTRACTION DES DONNÉES DE L'ACTIVITÉ
        # ====================================================

        activity_id = activity.get("activity_id")
        employee_id = activity.get("employee_id")
        sport_type = activity.get("sport_type")
        distance_meters = activity.get("distance_meters")
        comment = activity.get("comment")

        start_raw = activity.get("start_datetime")
        end_raw = activity.get("end_datetime")


        # ====================================================
        # CONVERSION DES DATES
        # ====================================================

        start_datetime = convert_debezium_timestamp(start_raw)
        end_datetime = convert_debezium_timestamp(end_raw)


        # ====================================================
        # CALCUL DE LA DURÉE
        # ====================================================

        duration_text = format_duration(
            start_datetime,
            end_datetime,
        )


        # ====================================================
        # FORMATAGE DE LA DISTANCE
        # ====================================================

        distance_text = format_distance(distance_meters)


        # ====================================================
        # ENRICHISSEMENT AVEC LES DONNÉES RH
        # ====================================================

        employee = get_employee_name(
            db_connection,
            employee_id,
        )

        if employee is None:
            print(
                f"Activité {activity_id} ignorée : "
                f"salarié {employee_id} introuvable."
            )
            continue

        first_name, last_name = employee


        # ====================================================
        # CONSTRUCTION DU FUTUR MESSAGE SLACK
        # ====================================================

        slack_message = build_slack_message(
            first_name=first_name,
            last_name=last_name,
            sport_type=sport_type,
            duration_text=duration_text,
            distance_text=distance_text,
            comment=comment,
        )


        # ====================================================
        # AFFICHAGE POUR VALIDATION
        # ====================================================

        print()
        print("=" * 60)
        print("NOUVELLE ACTIVITÉ DÉTECTÉE")
        print("=" * 60)

        print(f"Activity ID : {activity_id}")
        print(f"Salarié     : {first_name} {last_name}")
        print(f"Sport       : {sport_type}")
        print(
            "Début       : "
            f"{start_datetime.strftime('%d/%m/%Y %H:%M:%S')}"
        )
        print(
            "Fin         : "
            f"{end_datetime.strftime('%d/%m/%Y %H:%M:%S')}"
        )
        print(f"Durée       : {duration_text}")
        print(f"Distance    : {distance_text or 'Non applicable'}")

        print()
        print("MESSAGE SLACK À ENVOYER")
        print("-" * 60)
        print(slack_message)
        print("-" * 60)

        # Envoi du message vers Slack.
        send_slack_message(slack_message)


# ============================================================
# ARRÊT PROPRE
# ============================================================

except KeyboardInterrupt:

    print("\nArrêt demandé par l'utilisateur.")


finally:

    consumer.close()
    db_connection.close()

    print("Consumer Redpanda arrêté.")