"""
Test minimal de l'Incoming Webhook Slack.

Ce script permet uniquement de vérifier que :
1. le fichier .env est correctement chargé ;
2. le webhook Slack fonctionne ;
3. un message peut être publié dans le channel configuré.

Il ne dépend ni de PostgreSQL, ni de Debezium, ni de Redpanda.
"""

import os

import requests
from dotenv import load_dotenv


# ============================================================
# CHARGEMENT DE LA CONFIGURATION
# ============================================================

# Charge les variables définies dans le fichier .env.
load_dotenv()

# Récupère l'URL secrète du webhook Slack.
SLACK_WEBHOOK_URL = os.getenv("SLACK_WEBHOOK_URL")


# Vérification de sécurité :
# le programme s'arrête si le webhook n'est pas configuré.
if not SLACK_WEBHOOK_URL:
    raise ValueError(
        "SLACK_WEBHOOK_URL est absente du fichier .env."
    )


# ============================================================
# MESSAGE DE TEST
# ============================================================

message = {
    "text": (
        "🏃 Test Sport Data Solution\n"
        "La connexion entre Python et Slack fonctionne ! ✅"
    )
}


# ============================================================
# ENVOI VERS SLACK
# ============================================================

try:

    response = requests.post(
        SLACK_WEBHOOK_URL,
        json=message,
        timeout=10,
    )

    # Déclenche une erreur si Slack répond avec
    # un code HTTP 4xx ou 5xx.
    response.raise_for_status()

    print("=" * 60)
    print("TEST SLACK")
    print("=" * 60)
    print("Message envoyé avec succès. ✅")
    print(f"Réponse Slack : {response.text}")


except requests.RequestException as error:

    print("=" * 60)
    print("ERREUR SLACK")
    print("=" * 60)
    print(error)