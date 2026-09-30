# Sport Data Solution

## Créer et automatiser une architecture de données

Projet réalisé dans le cadre du parcours **Data Engineer d'OpenClassrooms**.

## Contexte

Sport Data Solution souhaite mettre en place une architecture de données permettant de centraliser et d'exploiter les données RH et les activités sportives de ses salariés.

La solution développée permet notamment de :

- intégrer les données RH dans PostgreSQL ;
- générer des données d'activités sportives simulant une API de type Strava ;
- contrôler les trajets sportifs domicile-travail avec Google Maps ;
- capturer les nouvelles activités en temps réel avec Debezium ;
- transmettre les événements via Redpanda ;
- traiter les données avec PySpark ;
- stocker les données traitées dans Delta Lake ;
- calculer les avantages attribués aux salariés ;
- envoyer des notifications Slack lors de nouvelles activités ;
- restituer les indicateurs métier dans Power BI.

## Technologies utilisées

- Python
- PostgreSQL
- Docker / Docker Compose
- Debezium
- Redpanda
- Apache Spark / PySpark
- Delta Lake
- Google Maps API
- Slack Webhook
- Power BI
- Git / GitHub

## Architecture de la solution

L'architecture combine des traitements batch et un flux temps réel basé sur le CDC (Change Data Capture).

```text
Données RH / Données sportives
            |
            v
       PostgreSQL
            |
            | CDC
            v
        Debezium
            |
            v
        Redpanda
            |
            v
         PySpark
            |
            v
       Delta Lake
            |
            +--------------------+
            |                    |
            v                    v
   employee_benefits         activities
            |                    |
            +---------+----------+
                      |
                      v
                  Power BI

Services complémentaires :
- Google Maps API : validation des trajets sportifs domicile-travail
- Slack Webhook : notification des nouvelles activités sportives

```

## Fonctionnement du pipeline

1. Les données RH sont contrôlées puis chargées dans PostgreSQL.
2. Des activités sportives sont générées afin de simuler une source de données de type Strava.
3. Les trajets domicile-travail éligibles sont contrôlés avec Google Maps.
4. Debezium capture les changements de la table `public.activities`.
5. Les événements CDC sont publiés dans Redpanda.
6. PySpark consomme, contrôle et transforme les activités.
7. Les données sont stockées dans Delta Lake.
8. La table métier `employee_benefits` est construite à partir des données consolidées.
9. Les données finales sont exportées pour Power BI.
10. Un consumer dédié peut envoyer une notification Slack lorsqu'une nouvelle activité est détectée.

## Structure du projet

```text
.
├── data/
│   ├── Donnees_RH.xlsx
│   ├── Donnees_Sportive.xlsx
│   └── powerbi/
│       ├── activities.csv
│       └── employee_benefits.csv
│
├── debezium/
│   └── postgres-activities-connector.example.json
│
├── spark/
│   ├── 01_read_redpanda.py
│   ├── 02_parse_activities.py
│   ├── 04_stream_to_delta.py
│   ├── 07_merge_postgres_to_delta.py
│   ├── 08_quality_delta_activities.py
│   ├── 09_consume_activities.py
│   ├── 10_build_employee_benefits.py
│   ├── 11_export_powerbi.py
│   └── 12_monitor_pipeline.py
│
├── sql/
│   └── 01_create_tables.sql
│
├── src/
│   ├── 01_audit_donnees.py
│   ├── 03_ingest_excel.py
│   ├── 04_test_google_routes.py
│   ├── 05_validate_commutes.py
│   ├── 06_generate_activities.py
│   ├── 07_load_activities.py
│   ├── 08_check_activities_quality.py
│   └── 10_test_slack.py
│
├── .env.example
├── .gitignore
├── docker-compose.yml
├── requirements.txt
├── Sport_data.pbix
└── README.md
```

## Règles métier

### Prime sportive liée au trajet domicile-travail

La prime sportive est attribuée aux salariés dont le trajet domicile-travail est considéré comme sportif et a été validé.

Le traitement s'appuie sur les données RH et sur la validation des trajets réalisée avec Google Maps.

Pour un salarié éligible :

- `sport_bonus_eligible = true`
- le montant de la prime correspond à **5 % du salaire brut**
- sinon, le montant attribué est de **0 €**

### Journées bien-être

Les journées bien-être sont calculées à partir des activités sportives enregistrées au cours des **12 derniers mois**.

Un salarié est éligible lorsqu'il atteint au moins **15 activités sportives** sur cette période.

Pour un salarié éligible :

- `wellbeing_days_eligible = true`
- `wellbeing_days_awarded = 5`

Dans le cas contraire :

- `wellbeing_days_eligible = false`
- `wellbeing_days_awarded = 0`

### Table métier `employee_benefits`

La table Delta `employee_benefits` contient une ligne par salarié et centralise les informations nécessaires à la restitution Power BI :

- identité du salarié ;
- Business Unit ;
- salaire brut ;
- mode et distance du trajet domicile-travail ;
- validation du trajet sportif ;
- nombre d'activités sportives ;
- distance sportive cumulée ;
- éligibilité et montant de la prime sportive ;
- éligibilité et nombre de journées bien-être attribuées ;
- date de calcul.

## Résultats obtenus

Les contrôles réalisés sur le pipeline final ont permis de valider les résultats suivants :

| Indicateur | Résultat |
|---|---:|
| Salariés traités | 161 |
| Activités sportives dans Delta Lake | 2 550 |
| Salariés ayant au moins une activité sur les 12 derniers mois | 95 |
| Salariés éligibles à la prime sportive | 68 |
| Salariés non éligibles à la prime sportive | 93 |
| Montant total des primes sportives | 172 482,50 € |
| Salariés éligibles aux journées bien-être | 73 |
| Salariés non éligibles aux journées bien-être | 88 |
| Journées bien-être attribuées | 365 |

### Contrôles qualité

Le monitoring final du pipeline a également confirmé :

- 161 salariés dans PostgreSQL ;
- 2 550 activités dans PostgreSQL ;
- 2 550 activités dans Delta Lake ;
- aucun doublon sur `activity_id` ;
- aucun `activity_id` NULL ;
- aucun `employee_id` NULL dans les activités ;
- aucune date incohérente ;
- aucune distance négative ;
- aucun doublon sur `employee_id` dans `employee_benefits` ;
- aucune différence de volumétrie entre PostgreSQL et Delta Lake.

Le contrôle global retourne :

`ÉTAT GLOBAL DU PIPELINE : OK`

### Restitution Power BI

Deux jeux de données sont préparés automatiquement pour la restitution :

- `data/powerbi/employee_benefits.csv` : 161 lignes ;
- `data/powerbi/activities.csv` : 2 550 lignes.

Le fichier `Sport_data.pbix` contient la restitution Power BI du projet.

## Installation et configuration

### Prérequis

Pour exécuter le projet localement, les outils suivants sont nécessaires :

- Git ;
- Docker Desktop ;
- Python ;
- PowerShell ou un terminal équivalent ;
- Power BI Desktop pour consulter la restitution.

### 1. Récupérer le projet

```bash
git clone https://github.com/Fatima012020/Creez_et_automatisez_une_architecture_de_donnees_THIAM_Fatou.git
cd Creez_et_automatisez_une_architecture_de_donnees_THIAM_Fatou
```

L'URL du dépôt GitHub sera renseignée après la création du dépôt distant.

### 2. Configurer les variables d'environnement

Le fichier `.env` contenant les secrets n'est pas versionné.

Créer un fichier `.env` à partir de `.env.example` :

```env
POSTGRES_PASSWORD=VOTRE_MOT_DE_PASSE
GOOGLE_MAPS_API_KEY=VOTRE_CLE_API
SLACK_WEBHOOK_URL=VOTRE_WEBHOOK_SLACK
```

Ne jamais publier les valeurs réelles de ces variables sur GitHub.

### 3. Installer les dépendances Python

Créer et activer un environnement virtuel puis installer les dépendances :

```powershell
python -m venv .venv
.\.venv\Scripts\Activate.ps1
pip install -r requirements.txt
```

### 4. Démarrer l'infrastructure Docker

```powershell
docker compose up -d
```

L'infrastructure Docker fournit les principaux composants nécessaires au pipeline, notamment PostgreSQL, Redpanda, Debezium/Kafka Connect et Spark.

### 5. Configuration Debezium

Le fichier :

`debezium/postgres-activities-connector.example.json`

présente un exemple de configuration du connecteur CDC.

Le fichier de configuration local contenant les identifiants réels est volontairement exclu de Git.

## Exécution du pipeline

Les commandes suivantes correspondent aux principales étapes d'exécution et de contrôle du projet.

### 1. Démarrer les services Docker

```powershell
docker compose up -d
docker compose ps
```

### 2. Auditer les données sources

```powershell
python .\src\01_audit_donnees.py
```

### 3. Charger les données dans PostgreSQL

```powershell
python .\src\03_ingest_excel.py
```

### 4. Valider les trajets domicile-travail

```powershell
python .\src\05_validate_commutes.py
```

### 5. Générer les activités sportives simulées

```powershell
python .\src\06_generate_activities.py
```

### 6. Charger et contrôler les activités

```powershell
python .\src\07_load_activities.py
python .\src\08_check_activities_quality.py
```

### 7. Construire la table métier `employee_benefits`

```powershell
docker exec -it sport_data_spark /opt/spark/bin/spark-submit /opt/spark/work-dir/scripts/10_build_employee_benefits.py
```

Ce traitement construit une ligne par salarié et applique les règles d'attribution de la prime sportive et des journées bien-être.

### 8. Exporter les données pour Power BI

```powershell
docker exec -it sport_data_spark /opt/spark/bin/spark-submit /opt/spark/work-dir/scripts/11_export_powerbi.py
```

Les fichiers générés sont disponibles dans :

```text
data/powerbi/employee_benefits.csv
data/powerbi/activities.csv
```

### 9. Contrôler le pipeline

```powershell
docker exec -it sport_data_spark /opt/spark/bin/spark-submit /opt/spark/work-dir/scripts/12_monitor_pipeline.py
```

Le monitoring contrôle notamment les volumes, les doublons, les valeurs NULL, la cohérence des dates et la synchronisation entre PostgreSQL et Delta Lake.

### 10. Vérifier les dernières données Delta

```powershell
docker exec -it sport_data_spark /opt/spark/bin/spark-submit /opt/spark/work-dir/scripts/13_check_last_delta.py
```

### 11. Tester le flux temps réel et Slack

Le consumer écoute les nouvelles activités publiées dans Redpanda à partir du CDC Debezium :

```powershell
docker exec -it sport_data_spark /opt/spark/bin/spark-submit /opt/spark/work-dir/scripts/09_consume_activities.py
```

Lorsqu'une nouvelle activité est détectée, ses informations sont traitées et une notification peut être envoyée sur Slack.

## Sécurité et gestion des secrets

Les informations sensibles nécessaires au fonctionnement du projet sont stockées dans le fichier `.env`.

Ce fichier est exclu du versionnement grâce au `.gitignore` et ne doit jamais être publié sur GitHub.

Les secrets concernés sont notamment :

- le mot de passe PostgreSQL ;
- la clé Google Maps API ;
- le Webhook Slack.

Le fichier `.env.example` documente uniquement les variables nécessaires, sans contenir leurs valeurs réelles.

De la même manière, le fichier réel de configuration du connecteur Debezium n'est pas versionné. Seul le fichier d'exemple `debezium/postgres-activities-connector.example.json` est destiné au dépôt GitHub.

Les données générées par Spark et Delta Lake ainsi que les checkpoints de streaming sont également exclus du dépôt afin de ne versionner que le code et les éléments nécessaires à la reproduction du projet.
