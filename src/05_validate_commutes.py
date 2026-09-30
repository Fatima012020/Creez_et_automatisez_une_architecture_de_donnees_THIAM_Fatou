"""
Contrôle des trajets sportifs domicile -> entreprise.

Objectif :
    Vérifier la cohérence des moyens de déplacement sportifs
    déclarés par les salariés.

Règles métier :
    - Marche/running :
        distance maximale = 15 km
        mode Google = WALK

    - Vélo/Trottinette/Autres :
        distance maximale = 25 km
        mode Google = BICYCLE

Le script :
    1. récupère dans PostgreSQL les salariés concernés ;
    2. calcule leur distance domicile -> entreprise ;
    3. applique le seuil correspondant ;
    4. enregistre le résultat dans commute_validation.

Le chargement est rejouable grâce à ON CONFLICT.
"""

import os
from pathlib import Path

import psycopg
import requests
from dotenv import load_dotenv


# =========================================================
# 1. CONFIGURATION
# =========================================================

# Racine du projet.
PROJECT_DIR = Path(__file__).resolve().parent.parent

# Chargement des variables sensibles depuis .env.
load_dotenv(PROJECT_DIR / ".env")

# Configuration PostgreSQL.
DB_HOST = "localhost"
DB_PORT = os.getenv("POSTGRES_PORT", "5432")
DB_NAME = os.getenv("POSTGRES_DB")
DB_USER = os.getenv("POSTGRES_USER")
DB_PASSWORD = os.getenv("POSTGRES_PASSWORD")

# Clé Google Maps.
GOOGLE_API_KEY = os.getenv("GOOGLE_MAPS_API_KEY")

if not GOOGLE_API_KEY:
    raise ValueError(
        "GOOGLE_MAPS_API_KEY est absente du fichier .env."
    )


# =========================================================
# 2. PARAMÈTRES MÉTIER
# =========================================================

# Adresse de Sport Data Solution indiquée
# dans la note de cadrage.
COMPANY_ADDRESS = (
    "1362 Av. des Platanes, 34970 Lattes"
)

# Endpoint Google Routes API.
GOOGLE_ROUTES_URL = (
    "https://routes.googleapis.com/"
    "directions/v2:computeRoutes"
)

# Correspondance entre les valeurs du fichier RH,
# le mode Google utilisé et le seuil métier.
COMMUTE_RULES = {
    "Marche/running": {
        "google_mode": "WALK",
        "threshold_km": 15.0,
    },
    "Vélo/Trottinette/Autres": {
        "google_mode": "BICYCLE",
        "threshold_km": 25.0,
    },
}


# =========================================================
# 3. FONCTION DE CALCUL DE DISTANCE
# =========================================================

def get_distance_km(origin, travel_mode):
    """
    Calcule la distance entre le domicile du salarié
    et l'entreprise avec Google Routes API.

    Paramètres
    ----------
    origin : str
        Adresse du domicile.

    travel_mode : str
        Mode Google utilisé : WALK ou BICYCLE.

    Retour
    ------
    float
        Distance du trajet en kilomètres.

    None
        Si aucun itinéraire ne peut être calculé.
    """

    # Données envoyées à Google.
    payload = {
        "origin": {
            "address": origin
        },
        "destination": {
            "address": COMPANY_ADDRESS
        },
        "travelMode": travel_mode,
        "units": "METRIC",
    }

    # En-têtes nécessaires à Routes API.
    headers = {
        "Content-Type": "application/json",
        "X-Goog-Api-Key": GOOGLE_API_KEY,

        # Nous ne demandons que la distance.
        "X-Goog-FieldMask": "routes.distanceMeters",
    }

    # Appel HTTP à Google.
    response = requests.post(
        GOOGLE_ROUTES_URL,
        json=payload,
        headers=headers,
        timeout=30,
    )

    # Une erreur HTTP est transformée en exception.
    # Cela évite d'enregistrer une fausse distance.
    response.raise_for_status()

    data = response.json()

    routes = data.get("routes", [])

    # Aucun itinéraire trouvé.
    if not routes:
        return None

    # Google renvoie la distance en mètres.
    distance_meters = routes[0]["distanceMeters"]

    # Conversion mètres -> kilomètres.
    return distance_meters / 1000


# =========================================================
# 4. CONNEXION À POSTGRESQL
# =========================================================

print("=" * 60)
print("CONTRÔLE DES TRAJETS SPORTIFS")
print("=" * 60)

with psycopg.connect(
    host=DB_HOST,
    port=DB_PORT,
    dbname=DB_NAME,
    user=DB_USER,
    password=DB_PASSWORD,
) as connection:

    with connection.cursor() as cursor:

        # =================================================
        # 5. SÉLECTION DES SALARIÉS À CONTRÔLER
        # =================================================

        # Nous ne sélectionnons que les deux catégories
        # concernées par les seuils de la note de cadrage.
        cursor.execute(
            """
            SELECT
                employee_id,
                home_address,
                commute_mode
            FROM employees
            WHERE commute_mode IN (
                'Marche/running',
                'Vélo/Trottinette/Autres'
            )
            ORDER BY employee_id;
            """
        )

        employees = cursor.fetchall()

        print(
            f"\nSalariés à contrôler : {len(employees)}"
        )

        # Compteurs utiles pour le résumé final.
        valid_count = 0
        invalid_count = 0
        error_count = 0


        # =================================================
        # 6. TRAITEMENT DES SALARIÉS
        # =================================================

        for employee_id, home_address, commute_mode in employees:

            # Récupération de la règle correspondant
            # au moyen de déplacement.
            rule = COMMUTE_RULES[commute_mode]

            google_mode = rule["google_mode"]
            threshold_km = rule["threshold_km"]

            try:

                # Calcul de la distance avec Google.
                distance_km = get_distance_km(
                    home_address,
                    google_mode,
                )

                # -----------------------------------------
                # CAS : aucun itinéraire trouvé
                # -----------------------------------------

                if distance_km is None:

                    error_count += 1

                    print(
                        f"[ERREUR] salarié {employee_id} : "
                        "aucun itinéraire trouvé"
                    )

                    # On ne stocke pas une fausse distance.
                    continue


                # -----------------------------------------
                # APPLICATION DE LA RÈGLE MÉTIER
                # -----------------------------------------

                is_valid = distance_km <= threshold_km

                if is_valid:
                    valid_count += 1

                    validation_reason = (
                        f"Distance inférieure ou égale "
                        f"au seuil de {threshold_km:g} km."
                    )

                    status = "VALIDE"

                else:
                    invalid_count += 1

                    validation_reason = (
                        f"Distance supérieure au seuil "
                        f"de {threshold_km:g} km."
                    )

                    status = "ANOMALIE"


                # -----------------------------------------
                # 7. ENREGISTREMENT POSTGRESQL
                # -----------------------------------------

                # ON CONFLICT rend également cette étape
                # rejouable :
                #
                # - première exécution -> INSERT ;
                # - exécution suivante -> UPDATE.
                cursor.execute(
                    """
                    INSERT INTO commute_validation (
                        employee_id,
                        distance_km,
                        threshold_km,
                        is_sport_commute,
                        is_valid,
                        validation_reason
                    )
                    VALUES (
                        %s, %s, %s, %s, %s, %s
                    )

                    ON CONFLICT (employee_id)
                    DO UPDATE SET
                        distance_km =
                            EXCLUDED.distance_km,
                        threshold_km =
                            EXCLUDED.threshold_km,
                        is_sport_commute =
                            EXCLUDED.is_sport_commute,
                        is_valid =
                            EXCLUDED.is_valid,
                        validation_reason =
                            EXCLUDED.validation_reason;
                    """,
                    (
                        employee_id,
                        distance_km,
                        threshold_km,
                        True,
                        is_valid,
                        validation_reason,
                    ),
                )

                # Affichage permettant de suivre
                # simplement l'exécution.
                print(
                    f"[{status}] salarié {employee_id} | "
                    f"{commute_mode} | "
                    f"{distance_km:.2f} km | "
                    f"seuil {threshold_km:g} km"
                )


            # ---------------------------------------------
            # CAS : erreur API
            # ---------------------------------------------

            except requests.RequestException as error:

                error_count += 1

                print(
                    f"[ERREUR] salarié {employee_id} : "
                    f"{error}"
                )


        # =================================================
        # 8. VÉRIFICATION POSTGRESQL
        # =================================================

        cursor.execute(
            "SELECT COUNT(*) FROM commute_validation;"
        )

        stored_count = cursor.fetchone()[0]


# =========================================================
# 9. RÉSUMÉ FINAL
# =========================================================

print("\n" + "=" * 60)
print("RÉSUMÉ")
print("=" * 60)

print(f"Salariés contrôlés : {len(employees)}")
print(f"Trajets valides     : {valid_count}")
print(f"Anomalies           : {invalid_count}")
print(f"Erreurs API         : {error_count}")
print(f"Lignes enregistrées : {stored_count}")

print("\nContrôle des trajets terminé.")