import os
from pyspark.sql import SparkSession
from pyspark.sql.functions import col


# ============================================================
# CONFIGURATION
# ============================================================

POSTGRES_HOST = "postgres"
POSTGRES_PORT = "5432"
POSTGRES_DB = "sport_data_db"
POSTGRES_USER = "sport_user"

# Le mot de passe PostgreSQL est récupéré depuis les variables
# d'environnement afin de ne jamais stocker de secret dans Git.
POSTGRES_PASSWORD = os.getenv("POSTGRES_PASSWORD")

if not POSTGRES_PASSWORD:
    raise ValueError(
        "La variable d'environnement POSTGRES_PASSWORD est absente."
    )

POSTGRES_URL = (
    f"jdbc:postgresql://"
    f"{POSTGRES_HOST}:"
    f"{POSTGRES_PORT}/"
    f"{POSTGRES_DB}"
)

DELTA_ACTIVITIES_PATH = "/opt/spark/work-dir/delta/activities"
DELTA_BENEFITS_PATH = "/opt/spark/work-dir/delta/employee_benefits"


# ============================================================
# SPARK
# ============================================================

spark = (
    SparkSession.builder
    .appName("SportDataMonitoring")
    .getOrCreate()
)

spark.sparkContext.setLogLevel("WARN")


def read_postgres_table(table_name):
    """Lit une table PostgreSQL via JDBC."""
    return (
        spark.read
        .format("jdbc")
        .option("url", POSTGRES_URL)
        .option("dbtable", table_name)
        .option("user", POSTGRES_USER)
        .option("password", POSTGRES_PASSWORD)
        .option("driver", "org.postgresql.Driver")
        .load()
    )


print("\n" + "=" * 60)
print("MONITORING - SPORT DATA SOLUTION")
print("=" * 60)


# ============================================================
# 1. POSTGRESQL
# ============================================================

postgres_status = "OK"

try:
    employees = read_postgres_table("public.employees")
    postgres_activities = read_postgres_table("public.activities")

    employees_count = employees.count()
    postgres_activities_count = postgres_activities.count()

except Exception as exc:
    postgres_status = "ERROR"
    employees_count = None
    postgres_activities_count = None

    print(f"\nErreur PostgreSQL : {exc}")


# ============================================================
# 2. DELTA ACTIVITIES
# ============================================================

delta_activities_status = "OK"

try:
    delta_activities = spark.read.format("delta").load(
        DELTA_ACTIVITIES_PATH
    )

    delta_activities_count = delta_activities.count()

except Exception as exc:
    delta_activities_status = "ERROR"
    delta_activities_count = None

    print(f"\nErreur Delta activities : {exc}")


# ============================================================
# 3. DELTA EMPLOYEE_BENEFITS
# ============================================================

delta_benefits_status = "OK"

try:
    employee_benefits = spark.read.format("delta").load(
        DELTA_BENEFITS_PATH
    )

    benefits_count = employee_benefits.count()

except Exception as exc:
    delta_benefits_status = "ERROR"
    benefits_count = None

    print(f"\nErreur Delta employee_benefits : {exc}")


# ============================================================
# 4. AFFICHAGE DES STATUTS
# ============================================================

print("\n" + "=" * 60)
print("ÉTAT DES COMPOSANTS")
print("=" * 60)

print(f"PostgreSQL                : {postgres_status}")
print(f"Delta activities          : {delta_activities_status}")
print(f"Delta employee_benefits   : {delta_benefits_status}")


# ============================================================
# 5. VOLUMÉTRIE
# ============================================================

print("\n" + "=" * 60)
print("VOLUMÉTRIE")
print("=" * 60)

print(f"Salariés PostgreSQL       : {employees_count}")
print(f"Activités PostgreSQL      : {postgres_activities_count}")
print(f"Activités Delta           : {delta_activities_count}")
print(f"Employee benefits Delta   : {benefits_count}")


# ============================================================
# 6. COHÉRENCE DES VOLUMES
# ============================================================

volume_status = "OK"

if (
    postgres_activities_count is not None
    and delta_activities_count is not None
):

    difference = postgres_activities_count - delta_activities_count

    if difference == 0:
        volume_status = "OK"

    elif difference > 0:
        volume_status = "WARNING"

    else:
        volume_status = "WARNING"

    print(f"\nÉcart PostgreSQL / Delta  : {difference}")

else:
    volume_status = "ERROR"

# ============================================================
# 7. CONTRÔLES QUALITÉ
# ============================================================

quality_status = "OK"

print("\n" + "=" * 60)
print("CONTRÔLES QUALITÉ")
print("=" * 60)

try:
    # --------------------------------------------------------
    # ACTIVITIES
    # --------------------------------------------------------

    activities_total = delta_activities.count()

    activities_distinct = (
        delta_activities
        .select("activity_id")
        .distinct()
        .count()
    )

    duplicate_activity_id = (
        activities_total - activities_distinct
    )

    null_activity_id = (
        delta_activities
        .filter(col("activity_id").isNull())
        .count()
    )

    null_employee_id = (
        delta_activities
        .filter(col("employee_id").isNull())
        .count()
    )

    inconsistent_dates = (
        delta_activities
        .filter(
            col("end_datetime") < col("start_datetime")
        )
        .count()
    )

    negative_distances = (
        delta_activities
        .filter(col("distance_meters") < 0)
        .count()
    )

    # --------------------------------------------------------
    # EMPLOYEE_BENEFITS
    # --------------------------------------------------------

    benefits_total = employee_benefits.count()

    benefits_distinct = (
        employee_benefits
        .select("employee_id")
        .distinct()
        .count()
    )

    duplicate_benefit_employee_id = (
        benefits_total - benefits_distinct
    )

    null_benefit_employee_id = (
        employee_benefits
        .filter(col("employee_id").isNull())
        .count()
    )

    # --------------------------------------------------------
    # AFFICHAGE
    # --------------------------------------------------------

    print(f"Doublons activity_id          : {duplicate_activity_id}")
    print(f"activity_id NULL              : {null_activity_id}")
    print(f"employee_id NULL activities   : {null_employee_id}")
    print(f"Dates incohérentes            : {inconsistent_dates}")
    print(f"Distances négatives           : {negative_distances}")
    print(
        f"Doublons employee_benefits    : "
        f"{duplicate_benefit_employee_id}"
    )
    print(
        f"employee_id NULL benefits     : "
        f"{null_benefit_employee_id}"
    )

    # --------------------------------------------------------
    # VERDICT QUALITÉ
    # --------------------------------------------------------

    blocking_errors = (
        duplicate_activity_id
        + null_activity_id
        + null_employee_id
        + inconsistent_dates
        + negative_distances
        + duplicate_benefit_employee_id
        + null_benefit_employee_id
    )

    if blocking_errors > 0:
        quality_status = "ERROR"

except Exception as exc:
    quality_status = "ERROR"
    print(f"Erreur contrôle qualité : {exc}")


print(f"\nÉtat qualité                  : {quality_status}")

# ============================================================
# 8. ÉTAT GLOBAL
# ============================================================

if (
    postgres_status == "ERROR"
    or delta_activities_status == "ERROR"
    or delta_benefits_status == "ERROR"
    or quality_status == "ERROR"
):
    global_status = "ERROR"

elif volume_status == "WARNING":
    global_status = "WARNING"

else:
    global_status = "OK"


print("\n" + "=" * 60)
print("RÉSUMÉ MONITORING")
print("=" * 60)

print(f"État PostgreSQL             : {postgres_status}")
print(f"État Delta activities       : {delta_activities_status}")
print(f"État employee_benefits      : {delta_benefits_status}")
print(f"Cohérence volumétrie        : {volume_status}")
print(f"Qualité des données         : {quality_status}")
print(f"ÉTAT GLOBAL DU PIPELINE     : {global_status}")

print("=" * 60)


spark.stop()