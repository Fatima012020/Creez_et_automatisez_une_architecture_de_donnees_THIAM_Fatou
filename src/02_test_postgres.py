"""
Test de connexion à PostgreSQL.

Ce script vérifie que l'environnement Python du projet
peut se connecter à la base PostgreSQL exécutée dans Docker.

Aucune table et aucune donnée métier ne sont créées.
"""

import os
from dotenv import load_dotenv
import psycopg

# Charge les variables définies dans le fichier .env
load_dotenv()


# ---------------------------------------------------------
# 1. PARAMÈTRES DE CONNEXION
# ---------------------------------------------------------

# Les valeurs correspondent à l'environnement PostgreSQL
# local défini dans le fichier .env.
#
# Pour ce premier test uniquement, nous utilisons les valeurs
# locales connues. La gestion complète des variables
# d'environnement sera améliorée ensuite.
DB_HOST = "localhost"
DB_PORT = os.getenv("POSTGRES_PORT", "5433")
DB_NAME = "sport_data_db"
DB_USER = "sport_user"

# Récupération du mot de passe depuis une variable
# d'environnement afin de ne pas l'écrire dans le code.
DB_PASSWORD = os.getenv("POSTGRES_PASSWORD")


# ---------------------------------------------------------
# 2. VÉRIFICATION DU SECRET
# ---------------------------------------------------------

# Si la variable n'existe pas, le script s'arrête avec
# un message compréhensible.
if not DB_PASSWORD:
    raise ValueError(
        "La variable d'environnement POSTGRES_PASSWORD "
        "n'est pas définie."
    )


# ---------------------------------------------------------
# 3. TEST DE CONNEXION
# ---------------------------------------------------------

try:
    # Ouverture d'une connexion vers PostgreSQL.
    with psycopg.connect(
        host=DB_HOST,
        port=DB_PORT,
        dbname=DB_NAME,
        user=DB_USER,
        password=DB_PASSWORD,
    ) as connection:

        print("Connexion PostgreSQL réussie.")

        # Affichage de la base courante uniquement pour
        # confirmer que nous sommes connectés à la bonne base.
        with connection.cursor() as cursor:
            cursor.execute("SELECT current_database();")
            database = cursor.fetchone()[0]

            print(f"Base connectée : {database}")

except Exception as error:
    print("Échec de la connexion PostgreSQL.")
    print(f"Erreur : {error}")