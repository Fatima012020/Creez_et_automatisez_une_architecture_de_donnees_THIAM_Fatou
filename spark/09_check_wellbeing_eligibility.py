from pyspark.sql import SparkSession
from pyspark.sql import functions as F


# ============================================================
# PARAMÈTRES MÉTIER
# ============================================================

DELTA_PATH = "/opt/spark/work-dir/delta/activities"

MIN_ACTIVITIES = 15
WELLBEING_DAYS = 5


# ============================================================
# SESSION SPARK + DELTA
# ============================================================

spark = (
    SparkSession.builder
    .appName("CheckWellbeingEligibility")
    .getOrCreate()
)

spark.sparkContext.setLogLevel("WARN")


print("=" * 60)
print("CONTRÔLE ÉLIGIBILITÉ - JOURNÉES BIEN-ÊTRE")
print("=" * 60)
print(f"Source Delta             : {DELTA_PATH}")
print(f"Seuil minimum activités  : {MIN_ACTIVITIES}")
print(f"Journées attribuées      : {WELLBEING_DAYS}")


# ============================================================
# LECTURE DE DELTA ACTIVITIES
# ============================================================

activities = (
    spark.read
    .format("delta")
    .load(DELTA_PATH)
)

# ============================================================
# FILTRE : ACTIVITÉS DES 12 DERNIERS MOIS
# ============================================================
#
# La note de cadrage demande de calculer l'éligibilité
# aux journées bien-être sur une période annuelle.
#
# On conserve donc uniquement les activités comprises
# dans les 12 derniers mois.

activities_last_12_months = (
    activities
    .filter(
        F.col("start_datetime")
        >= F.add_months(F.current_timestamp(), -12)
    )
    .filter(
        F.col("start_datetime")
        <= F.current_timestamp()
    )
)

# ============================================================
# AGRÉGATION PAR SALARIÉ
# ============================================================

activity_counts = (
    activities_last_12_months
    .groupBy("employee_id")
    .agg(
        F.count("activity_id").alias("activity_count")
    )
)


# ============================================================
# APPLICATION DE LA RÈGLE MÉTIER
# ============================================================

eligibility = (
    activity_counts
    .withColumn(
        "wellbeing_days_eligible",
        F.col("activity_count") >= MIN_ACTIVITIES
    )
    .withColumn(
        "wellbeing_days_awarded",
        F.when(
            F.col("wellbeing_days_eligible"),
            F.lit(WELLBEING_DAYS)
        ).otherwise(F.lit(0))
    )
)


# ============================================================
# INDICATEURS DE CONTRÔLE
# ============================================================

employees_with_activities = eligibility.count()

eligible_employees = (
    eligibility
    .filter(F.col("wellbeing_days_eligible") == True)
    .count()
)

non_eligible_employees = (
    eligibility
    .filter(F.col("wellbeing_days_eligible") == False)
    .count()
)

total_days_awarded = (
    eligibility
    .agg(
        F.sum("wellbeing_days_awarded").alias("total")
    )
    .first()["total"]
)


# ============================================================
# AFFICHAGE DU RÉSULTAT
# ============================================================

print()
print("=" * 60)
print("RÉSULTATS")
print("=" * 60)

print(f"Salariés avec activités      : {employees_with_activities}")
print(f"Salariés avec < 15 activités : {non_eligible_employees}")
print(f"Salariés avec >= 15 activités: {eligible_employees}")
print(f"Journées bien-être attribuées: {total_days_awarded}")


# ============================================================
# DÉTAIL DES COMPTAGES
# ============================================================

print()
print("=" * 60)
print("DÉTAIL PAR SALARIÉ")
print("=" * 60)

(
    eligibility
    .orderBy(F.desc("activity_count"))
    .show(100, truncate=False)
)


# ============================================================
# CONTRÔLE DE COHÉRENCE
# ============================================================

expected_days = eligible_employees * WELLBEING_DAYS

print()
print("=" * 60)
print("CONTRÔLE")
print("=" * 60)

print(
    f"{eligible_employees} salariés éligibles "
    f"x {WELLBEING_DAYS} jours "
    f"= {expected_days} journées"
)

if total_days_awarded == expected_days:
    print("Contrôle journées bien-être : OK")
else:
    print("Contrôle journées bien-être : KO")


spark.stop()