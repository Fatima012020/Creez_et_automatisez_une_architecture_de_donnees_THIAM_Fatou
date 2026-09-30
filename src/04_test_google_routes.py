"""
Test minimal de Google Routes API.

Objectif :
    Vérifier qu'une adresse de domicile peut être utilisée
    pour calculer la distance jusqu'au bureau de
    Sport Data Solution.

Ce script :
    - effectue UN SEUL appel API ;
    - affiche la distance obtenue ;
    - n'écrit rien dans PostgreSQL.

Il sert uniquement à valider le fonctionnement de l'API
avant de traiter les salariés concernés.
"""

import os
from pathlib import Path

import requests
from dotenv import load_dotenv


# ---------------------------------------------------------
# 1. CONFIGURATION DU PROJET
# ---------------------------------------------------------

PROJECT_DIR = Path(__file__).resolve().parent.parent

# Chargement du fichier .env afin de récupérer la clé
# Google Maps sans l'écrire directement dans le script.
load_dotenv(PROJECT_DIR / ".env")

API_KEY = os.getenv("GOOGLE_MAPS_API_KEY")

if not API_KEY:
    raise ValueError(
        "La variable GOOGLE_MAPS_API_KEY est absente du fichier .env."
    )


# ---------------------------------------------------------
# 2. ADRESSES DU TEST
# ---------------------------------------------------------

# Pour ce premier test, on utilise une adresse présente
# dans les données RH.
#
# L'adresse de destination est celle de l'entreprise
# fournie dans la note de cadrage.
ORIGIN = "53 Av. de la Gare, 34970 Lattes"

DESTINATION = "1362 Av. des Platanes, 34970 Lattes"


# ---------------------------------------------------------
# 3. ENDPOINT GOOGLE ROUTES API
# ---------------------------------------------------------

URL = (
    "https://routes.googleapis.com/"
    "directions/v2:computeRoutes"
)


# ---------------------------------------------------------
# 4. CORPS DE LA REQUÊTE
# ---------------------------------------------------------

# Le salarié utilisé pour ce test déclare
# Marche/running.
#
# Nous demandons donc un itinéraire piéton.
payload = {
    "origin": {
        "address": ORIGIN
    },
    "destination": {
        "address": DESTINATION
    },
    "travelMode": "WALK",
    "units": "METRIC",
}


# ---------------------------------------------------------
# 5. EN-TÊTES HTTP
# ---------------------------------------------------------

headers = {
    "Content-Type": "application/json",

    # La clé reste dans .env et est transmise uniquement
    # au service Google.
    "X-Goog-Api-Key": API_KEY,

    # Nous demandons uniquement la distance.
    # Cela évite de récupérer des données inutiles.
    "X-Goog-FieldMask": "routes.distanceMeters",
}


# ---------------------------------------------------------
# 6. APPEL DE L'API
# ---------------------------------------------------------

print("Test Google Routes API")
print("-" * 50)
print(f"Départ      : {ORIGIN}")
print(f"Destination : {DESTINATION}")
print("Mode        : WALK")

response = requests.post(
    URL,
    json=payload,
    headers=headers,
    timeout=30,
)


# ---------------------------------------------------------
# 7. CONTRÔLE DE LA RÉPONSE
# ---------------------------------------------------------

if response.status_code != 200:
    print("\nÉchec de l'appel Google Routes API.")
    print(f"Code HTTP : {response.status_code}")
    print(f"Réponse   : {response.text}")

else:
    data = response.json()

    # Google peut retourner une liste vide lorsqu'aucun
    # itinéraire n'a pu être calculé.
    routes = data.get("routes", [])

    if not routes:
        print("\nAucun itinéraire trouvé.")

    else:
        # Google renvoie la distance en mètres.
        distance_meters = routes[0]["distanceMeters"]

        # Conversion en kilomètres pour faciliter
        # l'application de nos règles métier.
        distance_km = distance_meters / 1000

        print("\nAppel API réussi.")
        print(f"Distance : {distance_meters} mètres")
        print(f"Distance : {distance_km:.2f} km")