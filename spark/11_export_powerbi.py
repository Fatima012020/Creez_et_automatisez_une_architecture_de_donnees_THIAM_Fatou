"""
11_export_powerbi.py

Étape 8.11.3 - Sport Data Solution

Objectif :
    Préparer les données Delta Lake pour leur restitution
    dans Power BI.

Sources :
    - Delta activities
    - Delta employee_benefits

Sorties :
    - employee_benefits.csv
    - activities.csv

IMPORTANT :
    Ce script ne réalise aucun nouveau calcul métier.

    Les règles d'attribution des avantages ont déjà été
    appliquées par Spark lors de la construction de
    employee_benefits.

    Ce traitement sert uniquement de couche d'exposition
    entre Delta Lake et Power BI.
"""

import os
import shutil

from pyspark.sql import SparkSession
from pyspark.sql import functions as F


# ============================================================
# CHEMINS
# ============================================================

ACTIVITIES_DELTA_PATH = (
    "/opt/spark/work-dir/delta/activities"
)

EMPLOYEE_BENEFITS_DELTA_PATH = (
    "/opt/spark/work-dir/delta/employee_benefits"
)

POWERBI_OUTPUT_PATH = (
    "/opt/spark/work-dir/powerbi"
)

TEMP_BENEFITS_PATH = (
    f"{POWERBI_OUTPUT_PATH}/_temp_employee_benefits"
)

TEMP_ACTIVITIES_PATH = (
    f"{POWERBI_OUTPUT_PATH}/_temp_activities"
)


# ============================================================
# SESSION SPARK
# ============================================================

spark = (
    SparkSession.builder
    .appName("SportData-Export-PowerBI")
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


print("=" * 60)
print("EXPORT DELTA LAKE -> POWER BI")
print("=" * 60)


# ============================================================
# 1. LECTURE DE EMPLOYEE_BENEFITS
# ============================================================

print()
print("Lecture de Delta employee_benefits...")

employee_benefits = (
    spark.read
    .format("delta")
    .load(EMPLOYEE_BENEFITS_DELTA_PATH)
)


# ============================================================
# 2. CONTRÔLE EMPLOYEE_BENEFITS
# ============================================================

benefits_count = employee_benefits.count()

benefits_distinct = (
    employee_benefits
    .select("employee_id")
    .distinct()
    .count()
)

benefits_duplicates = (
    benefits_count - benefits_distinct
)


print()
print("=" * 60)
print("CONTRÔLE EMPLOYEE_BENEFITS")
print("=" * 60)

print(
    f"Lignes                 : {benefits_count}"
)

print(
    f"Employee ID distincts  : {benefits_distinct}"
)

print(
    f"Doublons employee_id   : {benefits_duplicates}"
)


if (
    benefits_count != 161
    or benefits_distinct != 161
    or benefits_duplicates != 0
):
    raise ValueError(
        "Contrôle employee_benefits KO. "
        "Export Power BI annulé."
    )


print("Contrôle employee_benefits : OK")


# ============================================================
# 3. SÉLECTION DES COLONNES POUR POWER BI
# ============================================================
#
# Aucun calcul métier n'est effectué ici.
#
# On sélectionne uniquement les colonnes nécessaires
# à la restitution.
# ============================================================

benefits_powerbi = (
    employee_benefits
    .select(
        "employee_id",
        "first_name",
        "last_name",
        "business_unit",
        "gross_salary",
        "commute_mode",
        "commute_distance_km",
        "is_sport_commute",
        "commute_validated",
        "activity_count",
        "total_distance_meters",
        "sport_bonus_eligible",
        "sport_bonus_amount",
        "wellbeing_days_eligible",
        "wellbeing_days_awarded",
        "calculation_date"
    )
)


# ============================================================
# 4. LECTURE DE DELTA ACTIVITIES
# ============================================================

print()
print("Lecture de Delta activities...")

activities = (
    spark.read
    .format("delta")
    .load(ACTIVITIES_DELTA_PATH)
)


# ============================================================
# 5. CONTRÔLES ACTIVITIES
# ============================================================

activities_count = activities.count()

activities_distinct = (
    activities
    .select("activity_id")
    .distinct()
    .count()
)

activities_duplicates = (
    activities_count - activities_distinct
)


print()
print("=" * 60)
print("CONTRÔLE ACTIVITIES")
print("=" * 60)

print(
    f"Activités              : {activities_count}"
)

print(
    f"Activity ID distincts  : {activities_distinct}"
)

print(
    f"Doublons activity_id   : {activities_duplicates}"
)


if activities_duplicates != 0:
    raise ValueError(
        "Des doublons activity_id ont été détectés. "
        "Export Power BI annulé."
    )


print("Contrôle activities : OK")


# ============================================================
# 6. SÉLECTION ACTIVITIES POUR POWER BI
# ============================================================
#
# Cette deuxième table permettra notamment d'analyser :
#
# - les pratiques sportives ;
# - le nombre d'activités ;
# - les distances ;
# - les activités dans le temps.
# ============================================================

activities_powerbi = (
    activities
    .select(
        "activity_id",
        "employee_id",
        "start_datetime",
        "sport_type",
        "distance_meters",
        "end_datetime"
    )
)


# ============================================================
# 7. PRÉPARATION DU DOSSIER POWER BI
# ============================================================

os.makedirs(
    POWERBI_OUTPUT_PATH,
    exist_ok=True
)


# ============================================================
# 8. FONCTION D'EXPORT EN UN SEUL CSV
# ============================================================
#
# Spark écrit normalement un dossier contenant :
#
#     part-00000-....csv
#
# Pour Power BI, nous voulons un fichier simple :
#
#     employee_benefits.csv
#
# ou :
#
#     activities.csv
#
# On écrit donc d'abord dans un dossier temporaire,
# puis on renomme le fichier généré par Spark.
# ============================================================

def export_single_csv(
    dataframe,
    temp_path,
    final_filename
):
    """
    Exporte un DataFrame Spark dans un fichier CSV unique.

    Parameters
    ----------
    dataframe : pyspark.sql.DataFrame
        DataFrame à exporter.

    temp_path : str
        Répertoire temporaire utilisé par Spark.

    final_filename : str
        Nom final du fichier CSV.
    """

    final_path = os.path.join(
        POWERBI_OUTPUT_PATH,
        final_filename
    )

    # Nettoyage d'un ancien dossier temporaire.
    if os.path.exists(temp_path):
        shutil.rmtree(temp_path)

    # Suppression de l'ancien fichier final
    # afin que l'export soit rejouable.
    if os.path.exists(final_path):
        os.remove(final_path)

    # coalesce(1) force un seul fichier CSV.
    (
        dataframe
        .coalesce(1)
        .write
        .mode("overwrite")
        .option("header", "true")
        .option("encoding", "UTF-8")
        .csv(temp_path)
    )

    # Recherche du fichier part-*.csv créé par Spark.
    csv_files = [
        filename
        for filename in os.listdir(temp_path)
        if filename.startswith("part-")
        and filename.endswith(".csv")
    ]

    if len(csv_files) != 1:
        raise RuntimeError(
            "Impossible d'identifier le fichier CSV "
            "produit par Spark."
        )

    source_file = os.path.join(
        temp_path,
        csv_files[0]
    )

    # Déplacement vers le nom définitif.
    shutil.move(
        source_file,
        final_path
    )

    # Suppression du dossier temporaire Spark.
    shutil.rmtree(temp_path)

    return final_path


# ============================================================
# 9. EXPORT EMPLOYEE_BENEFITS
# ============================================================

print()
print("=" * 60)
print("EXPORT EMPLOYEE_BENEFITS")
print("=" * 60)

benefits_file = export_single_csv(
    benefits_powerbi,
    TEMP_BENEFITS_PATH,
    "employee_benefits.csv"
)

print(
    f"Fichier créé : {benefits_file}"
)


# ============================================================
# 10. EXPORT ACTIVITIES
# ============================================================

print()
print("=" * 60)
print("EXPORT ACTIVITIES")
print("=" * 60)

activities_file = export_single_csv(
    activities_powerbi,
    TEMP_ACTIVITIES_PATH,
    "activities.csv"
)

print(
    f"Fichier créé : {activities_file}"
)


# ============================================================
# 11. VERDICT
# ============================================================

print()
print("=" * 60)
print("VERDICT")
print("=" * 60)

print("EXPORT POWER BI OK.")
print()
print(
    f"employee_benefits : {benefits_count} lignes"
)

print(
    f"activities        : {activities_count} lignes"
)

print()
print(
    "Les données sont prêtes pour être importées "
    "dans Power BI."
)


# ============================================================
# FIN
# ============================================================

spark.stop()