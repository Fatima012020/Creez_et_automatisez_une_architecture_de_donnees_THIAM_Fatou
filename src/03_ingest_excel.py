"""
Ingestion des données Excel vers PostgreSQL.

Ce script réalise les opérations suivantes :

1. Lecture des fichiers Donnees_RH.xlsx et Donnees_Sportive.xlsx.
2. Transformation minimale des données.
3. Chargement des salariés dans la table employees.
4. Chargement des profils sportifs dans la table sport_profile.
5. Vérification du nombre de lignes insérées.

Les fichiers Excel originaux ne sont jamais modifiés.
"""

import os
from pathlib import Path

import pandas as pd
import psycopg
from dotenv import load_dotenv


# ---------------------------------------------------------
# 1. DÉFINITION DES CHEMINS DU PROJET
# ---------------------------------------------------------

# Le script se trouve dans "src".
# parent.parent permet de retrouver la racine du projet.
PROJECT_DIR = Path(__file__).resolve().parent.parent

# Chemins vers les fichiers Excel bruts.
RH_FILE = PROJECT_DIR / "data" / "Donnees_RH.xlsx"
SPORT_FILE = PROJECT_DIR / "data" / "Donnees_Sportive.xlsx"

# Chemin vers le fichier contenant les variables
# d'environnement utilisées pour PostgreSQL.
ENV_FILE = PROJECT_DIR / ".env"


# ---------------------------------------------------------
# 2. CHARGEMENT DES VARIABLES D'ENVIRONNEMENT
# ---------------------------------------------------------

# Lecture du fichier .env.
# Cela permet de ne pas écrire le mot de passe PostgreSQL
# directement dans le code Python.
load_dotenv(ENV_FILE)

DB_HOST = "localhost"
DB_PORT = os.getenv("POSTGRES_PORT", "5432")
DB_NAME = os.getenv("POSTGRES_DB")
DB_USER = os.getenv("POSTGRES_USER")
DB_PASSWORD = os.getenv("POSTGRES_PASSWORD")


# ---------------------------------------------------------
# 3. LECTURE DES FICHIERS EXCEL
# ---------------------------------------------------------

print("Lecture des fichiers Excel...")

# Chargement des données RH dans un DataFrame Pandas.
df_rh = pd.read_excel(RH_FILE)

# Chargement des données sportives.
df_sport = pd.read_excel(SPORT_FILE)

print(f"Données RH chargées      : {len(df_rh)} lignes")
print(f"Données Sport chargées   : {len(df_sport)} lignes")


# ---------------------------------------------------------
# 4. TRANSFORMATION DES DONNÉES RH
# ---------------------------------------------------------

# Les noms provenant d'Excel contiennent des espaces,
# des accents et sont en français.
#
# On les renomme pour utiliser des noms simples et
# homogènes dans PostgreSQL.
df_rh = df_rh.rename(
    columns={
        "ID salarié": "employee_id",
        "Nom": "last_name",
        "Prénom": "first_name",
        "Date de naissance": "birth_date",
        "BU": "business_unit",
        "Date d'embauche": "hire_date",
        "Salaire brut": "gross_salary",
        "Type de contrat": "contract_type",
        "Nombre de jours de CP": "paid_leave_days",
        "Adresse du domicile": "home_address",
        "Moyen de déplacement": "commute_mode",
    }
)


# ---------------------------------------------------------
# 5. TRANSFORMATION DES DONNÉES SPORTIVES
# ---------------------------------------------------------

# Renommage des colonnes du fichier sportif.
df_sport = df_sport.rename(
    columns={
        "ID salarié": "employee_id",
        "Pratique d'un sport": "sport_type",
    }
)

# L'audit initial a permis d'identifier une faute
# d'orthographe dans la valeur "Runing".
# On standardise cette valeur en "Running".
df_sport["sport_type"] = df_sport["sport_type"].replace(
    {"Runing": "Running"}
)

# Pandas représente les cellules Excel vides par NaN.
# PostgreSQL doit recevoir NULL et non la chaîne "NaN".
#
# La conversion en type "object" permet à Pandas de
# conserver réellement la valeur Python None.
df_sport["sport_type"] = (
    df_sport["sport_type"]
    .astype(object)
    .where(df_sport["sport_type"].notna(), None)
)


# ---------------------------------------------------------
# 6. PRÉPARATION DES DONNÉES À INSÉRER
# ---------------------------------------------------------

# Conversion de chaque ligne RH en tuple.
# L'ordre des valeurs correspond exactement à l'ordre
# des colonnes utilisé dans la requête INSERT plus bas.
employees_data = list(
    df_rh[
        [
            "employee_id",
            "last_name",
            "first_name",
            "birth_date",
            "business_unit",
            "hire_date",
            "gross_salary",
            "contract_type",
            "paid_leave_days",
            "home_address",
            "commute_mode",
        ]
    ].itertuples(index=False, name=None)
)

# Même principe pour les profils sportifs.
sport_data = list(
    df_sport[
        [
            "employee_id",
            "sport_type",
        ]
    ].itertuples(index=False, name=None)
)


# ---------------------------------------------------------
# 7. CONNEXION À POSTGRESQL
# ---------------------------------------------------------

print("\nConnexion à PostgreSQL...")

try:

    # Ouverture de la connexion.
    with psycopg.connect(
        host=DB_HOST,
        port=DB_PORT,
        dbname=DB_NAME,
        user=DB_USER,
        password=DB_PASSWORD,
    ) as connection:

        print("Connexion PostgreSQL réussie.")

        # Création d'un curseur.
        # Le curseur permet d'envoyer des commandes SQL
        # depuis Python vers PostgreSQL.
        with connection.cursor() as cursor:


            # -------------------------------------------------
            # 8. CHARGEMENT DE LA TABLE EMPLOYEES
            # -------------------------------------------------

            print("\nInsertion dans employees...")

            # -------------------------------------------------
            # CHARGEMENT REJOUABLE DE LA TABLE EMPLOYEES
            # -------------------------------------------------
            #
            # ON CONFLICT permet de gérer le cas où employee_id
            # existe déjà dans PostgreSQL.
            #
            # - Si le salarié n'existe pas : INSERT.
            # - S'il existe déjà : UPDATE avec les nouvelles
            #   valeurs provenant du fichier Excel.
            #
            # Cette stratégie rend l'ingestion rejouable sans
            # créer de doublons.

            cursor.executemany(
                """
                INSERT INTO employees (
                    employee_id,
                    last_name,
                    first_name,
                    birth_date,
                    business_unit,
                    hire_date,
                    gross_salary,
                    contract_type,
                    paid_leave_days,
                    home_address,
                    commute_mode
                )
                VALUES (
                    %s, %s, %s, %s, %s, %s,
                    %s, %s, %s, %s, %s
                )

                ON CONFLICT (employee_id)
                DO UPDATE SET
                    last_name = EXCLUDED.last_name,
                    first_name = EXCLUDED.first_name,
                    birth_date = EXCLUDED.birth_date,
                    business_unit = EXCLUDED.business_unit,
                    hire_date = EXCLUDED.hire_date,
                    gross_salary = EXCLUDED.gross_salary,
                    contract_type = EXCLUDED.contract_type,
                    paid_leave_days = EXCLUDED.paid_leave_days,
                    home_address = EXCLUDED.home_address,
                    commute_mode = EXCLUDED.commute_mode;
                """,
                employees_data,
            )

            print(
                f"{len(employees_data)} salariés traités "
                "(insertion ou mise à jour)."
            )


            # -------------------------------------------------
            # 9. CHARGEMENT DE SPORT_PROFILE
            # -------------------------------------------------

            print("\nInsertion dans sport_profile...")

            # -------------------------------------------------
            # CHARGEMENT REJOUABLE DE SPORT_PROFILE
            # -------------------------------------------------
            #
            # Comme employee_id est également la clé primaire
            # de sport_profile :
            #
            # - nouveau salarié sportif -> INSERT ;
            # - profil déjà présent -> UPDATE.
            #
            # Cela permet notamment de prendre en compte une
            # modification de la pratique sportive dans la source.

            cursor.executemany(
                """
                INSERT INTO sport_profile (
                    employee_id,
                    sport_type
                )
                VALUES (%s, %s)

                ON CONFLICT (employee_id)
                DO UPDATE SET
                    sport_type = EXCLUDED.sport_type;
                """,
                sport_data,
            )

            print(
                f"{len(sport_data)} profils sportifs traités "
                "(insertion ou mise à jour)."
            )


            # -------------------------------------------------
            # 10. VÉRIFICATION DU CHARGEMENT
            # -------------------------------------------------

            # On demande directement à PostgreSQL combien
            # de lignes sont réellement présentes dans
            # chacune des deux tables.

            cursor.execute("SELECT COUNT(*) FROM employees;")
            employees_count = cursor.fetchone()[0]

            cursor.execute("SELECT COUNT(*) FROM sport_profile;")
            sport_count = cursor.fetchone()[0]

            print("\nVérification PostgreSQL :")
            print(
                f"- employees     : {employees_count} lignes"
            )
            print(
                f"- sport_profile : {sport_count} lignes"
            )


    # ---------------------------------------------------------
    # 11. FIN DU TRAITEMENT
    # ---------------------------------------------------------

    print("\nIngestion terminée avec succès.")


except Exception as error:

    # En cas d'erreur, on affiche le message retourné
    # afin de faciliter le diagnostic.
    print("\nErreur pendant l'ingestion.")
    print(f"Détail : {error}")