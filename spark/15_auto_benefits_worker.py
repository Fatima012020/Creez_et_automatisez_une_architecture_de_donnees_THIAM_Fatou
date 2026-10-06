"""
15_auto_benefits_worker.py

Worker automatique du pipeline métier Sport Data Solution.

Pipeline :
    Delta activities
        ↓
    employee_benefits
        ↓
    contrôles qualité
        ↓
    CSV Power BI

Résilience :
    - surveillance continue des versions Delta ;
    - checkpoint persistant de la dernière version traitée ;
    - une version n'est validée qu'après succès complet ;
    - en cas d'erreur, aucun checkpoint n'est avancé ;
    - nouvelle tentative automatique ;
    - reprise correcte après redémarrage du conteneur.
"""

import json
import os
import subprocess
import time
import urllib.request
from datetime import datetime
from pathlib import Path


# ============================================================
# CONFIGURATION
# ============================================================

DELTA_LOG_PATH = Path(
    "/opt/spark/work-dir/delta/activities/_delta_log"
)

SCRIPT_BENEFITS = (
    "/opt/spark/work-dir/scripts/10_build_employee_benefits.py"
)

SCRIPT_EXPORT = (
    "/opt/spark/work-dir/scripts/11_export_powerbi.py"
)

SPARK_SUBMIT = "/opt/spark/bin/spark-submit"

IVY_PATH = "/opt/spark/.ivy2"

# Le fichier est placé dans le volume checkpoints déjà persistant.
STATE_FILE = Path(
    "/opt/spark/work-dir/checkpoints/"
    "benefits_worker_last_version.txt"
)

CHECK_INTERVAL_SECONDS = 10
RETRY_DELAY_SECONDS = 30

# Webhook Slack fourni par le fichier .env via Docker Compose.
SLACK_WEBHOOK_URL = os.getenv("SLACK_WEBHOOK_URL")


# ============================================================
# LOGS
# ============================================================

def log(message):
    """Affiche un message horodaté immédiatement."""
    timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    print(f"[{timestamp}] {message}", flush=True)

# ============================================================
# ALERTING SLACK
# ============================================================

def send_slack_alert(message):
    """
    Envoie une alerte technique dans Slack.

    Une erreur Slack ne doit jamais interrompre le pipeline :
    l'alerting est un mécanisme de supervision, pas une
    dépendance du traitement métier.
    """

    if not SLACK_WEBHOOK_URL:
        log(
            "Alerte Slack non envoyée : "
            "SLACK_WEBHOOK_URL non configuré."
        )
        return False

    try:
        payload = json.dumps(
            {"text": message}
        ).encode("utf-8")

        request = urllib.request.Request(
            SLACK_WEBHOOK_URL,
            data=payload,
            headers={
                "Content-Type": "application/json"
            },
            method="POST",
        )

        with urllib.request.urlopen(
            request,
            timeout=10,
        ) as response:

            if 200 <= response.status < 300:
                log("Alerte Slack envoyée.")
                return True

            log(
                "Slack a répondu avec le statut "
                f"{response.status}."
            )

    except Exception as error:
        log(
            "Échec de l'envoi de l'alerte Slack : "
            f"{type(error).__name__}: {error}"
        )

    return False

# ============================================================
# DELTA LAKE
# ============================================================

def get_latest_delta_version():
    """Retourne la dernière version disponible de Delta activities."""

    if not DELTA_LOG_PATH.exists():
        return None

    versions = []

    for delta_file in DELTA_LOG_PATH.glob("*.json"):
        try:
            versions.append(int(delta_file.stem))
        except ValueError:
            continue

    if not versions:
        return None

    return max(versions)


# ============================================================
# CHECKPOINT MÉTIER
# ============================================================

def read_last_processed_version():
    """
    Lit la dernière version Delta réellement traitée.

    Retourne None si aucun checkpoint métier n'existe encore.
    """

    if not STATE_FILE.exists():
        return None

    try:
        content = STATE_FILE.read_text(
            encoding="utf-8"
        ).strip()

        if not content:
            return None

        return int(content)

    except (ValueError, OSError) as error:
        log(
            "Checkpoint métier illisible : "
            f"{error}. Il sera ignoré."
        )
        return None


def save_last_processed_version(version):
    """
    Sauvegarde une version uniquement après succès
    complet du pipeline métier.
    """

    STATE_FILE.parent.mkdir(
        parents=True,
        exist_ok=True,
    )

    # Écriture temporaire puis remplacement :
    # évite de laisser un fichier partiellement écrit.
    temporary_file = STATE_FILE.with_suffix(".tmp")

    temporary_file.write_text(
        str(version),
        encoding="utf-8",
    )

    temporary_file.replace(STATE_FILE)

    log(
        "Checkpoint métier enregistré : "
        f"version Delta {version}"
    )


# ============================================================
# EXÉCUTION SPARK
# ============================================================

def run_spark_script(script_path, packages):
    """Exécute un traitement Spark et contrôle son résultat."""

    command = [
        SPARK_SUBMIT,
        "--master",
        "local[*]",
        "--packages",
        packages,
        "--conf",
        f"spark.jars.ivy={IVY_PATH}",
        script_path,
    ]

    log(f"Lancement : {Path(script_path).name}")

    subprocess.run(
        command,
        check=True,
        env=os.environ.copy(),
    )

    log(f"Succès : {Path(script_path).name}")


def run_business_pipeline():
    """
    Exécute les traitements dans l'ordre.

    L'export Power BI ne peut commencer que si
    employee_benefits a été calculé avec succès.
    """

    run_spark_script(
        SCRIPT_BENEFITS,
        (
            "io.delta:delta-spark_2.12:3.2.0,"
            "org.postgresql:postgresql:42.7.4"
        ),
    )

    run_spark_script(
        SCRIPT_EXPORT,
        "io.delta:delta-spark_2.12:3.2.0",
    )


# ============================================================
# INITIALISATION
# ============================================================

def initialise_checkpoint():
    """
    Initialise le checkpoint au premier déploiement.

    Si aucun fichier d'état n'existe encore, la version Delta
    actuelle est considérée comme déjà traitée, puisque le
    pipeline avait été validé avant l'installation du checkpoint.
    """

    saved_version = read_last_processed_version()

    if saved_version is not None:
        return saved_version

    current_version = get_latest_delta_version()

    if current_version is not None:
        save_last_processed_version(current_version)

        log(
            "Premier démarrage avec checkpoint persistant : "
            f"version actuelle {current_version} initialisée."
        )

    return current_version


# ============================================================
# WORKER
# ============================================================

def main():
    """Surveille Delta et automatise les traitements métier."""

    log("=" * 60)
    log("WORKER AUTOMATIQUE EMPLOYEE_BENEFITS / POWER BI")
    log("=" * 60)

    last_processed_version = initialise_checkpoint()

    failed_version = None

    log(
        "Dernière version Delta traitée : "
        f"{last_processed_version}"
    )

    log("Surveillance des nouvelles activités...")

    while True:

        try:
            current_version = get_latest_delta_version()

            if current_version is None:

                log(
                    "Table Delta activities indisponible. "
                    "Nouvelle vérification ultérieure."
                )

            elif (
                last_processed_version is None
                or current_version > last_processed_version
            ):

                log(
                    "Nouvelle version Delta détectée : "
                    f"{current_version}"
                )

                log(
                    "Dernière version validée : "
                    f"{last_processed_version}"
                )

                # Important :
                # aucun checkpoint n'est enregistré avant
                # la réussite de l'intégralité du pipeline.
                run_business_pipeline()

                save_last_processed_version(
                    current_version
                )

                last_processed_version = current_version

                if failed_version == current_version:

                    send_slack_alert(
                        "✅ PIPELINE SPORT DATA RÉTABLI\n"
                        f"Version Delta : {current_version}\n"
                        "Le traitement métier a finalement réussi.\n"
                        "Le checkpoint a été mis à jour et "
                        "la surveillance automatique reprend."
                    )

                    failed_version = None

                log(
                    "Pipeline métier terminé avec succès "
                    f"pour la version Delta {current_version}."
                )

                log("Surveillance reprise.")

            time.sleep(CHECK_INTERVAL_SECONDS)

        except subprocess.CalledProcessError as error:

            log("=" * 60)
            log("ÉCHEC DU PIPELINE MÉTIER")
            log("=" * 60)

            log(
                "Un traitement Spark a échoué "
                f"(code retour {error.returncode})."
            )

            log(
                "Le checkpoint métier N'EST PAS modifié."
            )

            log(
                "La version Delta reste à retraiter."
            )

            log(
                f"Nouvelle tentative automatique dans "
                f"{RETRY_DELAY_SECONDS} secondes."
            )
            failed_version = current_version

            send_slack_alert(
                "🚨 ALERTE PIPELINE SPORT DATA\n"
                f"Échec du traitement de la version Delta "
                f"{current_version}.\n"
                f"Code retour Spark : {error.returncode}\n"
                "Le checkpoint métier n'a pas été modifié.\n"
                f"Nouvelle tentative automatique dans "
                f"{RETRY_DELAY_SECONDS} secondes."
            )

            time.sleep(RETRY_DELAY_SECONDS)

        except Exception as error:

            log("=" * 60)
            log("ERREUR INATTENDUE DU WORKER")
            log("=" * 60)

            log(
                f"{type(error).__name__}: {error}"
            )

            log(
                "Le checkpoint métier N'EST PAS modifié."
            )

            log(
                f"Nouvelle tentative automatique dans "
                f"{RETRY_DELAY_SECONDS} secondes."
            )

            time.sleep(RETRY_DELAY_SECONDS)

# ============================================================
# TEST DE L'ALERTING
# ============================================================

def test_slack_alerting():
    """
    Teste les notifications techniques Slack sans modifier
    PostgreSQL, Delta Lake ou le checkpoint métier.
    """

    log("Test de l'alerting Slack...")

    send_slack_alert(
        "🚨 TEST ALERTE PIPELINE SPORT DATA\n"
        "Simulation d'un échec de traitement.\n"
        "Le checkpoint métier ne serait pas modifié.\n"
        "Une nouvelle tentative automatique serait programmée."
    )

    time.sleep(2)

    send_slack_alert(
        "✅ TEST RÉTABLISSEMENT PIPELINE SPORT DATA\n"
        "Simulation du retour à la normale.\n"
        "Le traitement peut reprendre automatiquement."
    )

    log("Test de l'alerting terminé.")

if __name__ == "__main__":

    if os.getenv("TEST_SLACK_ALERTING") == "1":
        test_slack_alerting()
    else:
        main()