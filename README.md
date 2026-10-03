# Sport Data Solution

## Créer et automatiser une architecture de données

Projet réalisé dans le cadre du parcours **Data Engineer d'OpenClassrooms**.

---

## Contexte

Sport Data Solution souhaite mettre en place une architecture de données permettant de centraliser, contrôler et exploiter les données RH et les activités sportives de ses salariés.

L'objectif du POC est notamment de :

- intégrer les données RH dans PostgreSQL ;
- générer des activités sportives simulant une source de type Strava ;
- contrôler les trajets sportifs domicile-travail avec Google Maps ;
- capturer les nouvelles activités en temps réel avec Debezium ;
- transmettre les événements via Redpanda ;
- traiter les données avec Python et PySpark ;
- envoyer des notifications Slack lors de nouvelles activités ;
- stocker les données analytiques dans Delta Lake ;
- calculer les avantages attribués aux salariés ;
- exporter les résultats métier ;
- restituer les indicateurs dans Power BI ;
- contrôler la qualité et la cohérence du pipeline.

---

## Technologies utilisées

- Python
- PostgreSQL
- Docker / Docker Compose
- Debezium
- Redpanda
- Apache Spark / PySpark
- Delta Lake
- Google Maps API
- Slack Incoming Webhook
- Power BI
- Git / GitHub

---

## Architecture de la solution

L'architecture combine un stockage PostgreSQL, un flux temps réel basé sur le **CDC (Change Data Capture)** et deux branches de traitement après Redpanda.

```text
                         PostgreSQL
                             │
                          Debezium
                             │
                          Redpanda
                         /        \
                        /          \
              Consumer Python      Spark
                    │                │
                  Slack          Delta Lake
                                     │
                              employee_benefits
                                     │
                                   CSV
                                     │
                                 Power BI
```

### Branche notification

```text
PostgreSQL
    │
Debezium
    │
Redpanda
    │
Consumer Python
    │
Slack
```

Cette branche permet de détecter une nouvelle activité sportive et d'envoyer une notification Slack.

### Branche analytique

```text
PostgreSQL
    │
Debezium
    │
Redpanda
    │
Spark
    │
Delta Lake
    │
employee_benefits
    │
CSV
    │
Power BI
```

Cette branche permet de contrôler, transformer et consolider les données avant leur restitution métier.

### Services complémentaires

- **Google Maps API** : validation des trajets sportifs domicile-travail ;
- **Monitoring** : contrôle des volumes, doublons, valeurs NULL et incohérences ;
- **Docker Compose** : orchestration de l'environnement technique local.

---

## Fonctionnement du pipeline

1. Les données RH sont auditées puis chargées dans PostgreSQL.
2. Des activités sportives sont générées afin de simuler une source de données de type Strava.
3. Les trajets domicile-travail éligibles sont contrôlés avec Google Maps.
4. Debezium surveille la table `public.activities` grâce au CDC.
5. Lorsqu'une activité est ajoutée ou modifiée, l'événement est publié dans Redpanda.
6. Deux traitements peuvent alors être exécutés :
   - un consumer Python récupère l'événement et peut envoyer une notification Slack ;
   - Spark traite les données destinées à la branche analytique.
7. Les données analytiques sont stockées dans Delta Lake.
8. La table métier `employee_benefits` est construite.
9. Les données finales sont exportées au format CSV.
10. Power BI utilise ces données pour restituer les indicateurs métier.
11. Des contrôles de monitoring vérifient la cohérence globale du pipeline.

---

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
│   ├── 10_build_employee_benefits.py
│   ├── 11_export_powerbi.py
│   ├── 12_monitor_pipeline.py
│   ├── 13_check_last_delta.py
│   └── 14_cleanup_delta.py
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
│   ├── 09_consume_activities.py
│   └── 10_test_slack.py
│
├── .env.example
├── .gitignore
├── docker-compose.yml
├── requirements.txt
├── Sport_data.pbix
├── Support_Soutenance_P12_Sport_Data_Solution_Fatou_THIAM.pdf
├── Support_Soutenance_P12_Sport_Data_Solution_Fatou_THIAM.pptx
└── README.md
```

> Les dossiers Delta Lake et les checkpoints Spark générés localement ne sont pas versionnés.

---

# Règles métier

## 1. Prime sportive liée au trajet domicile-travail

La prime sportive concerne les salariés dont le trajet domicile-travail est considéré comme sportif et validé.

La validation repose notamment sur les informations RH et le contrôle du trajet avec Google Maps.

Pour un salarié éligible :

- `sport_bonus_eligible = true` ;
- le montant potentiel de la prime correspond à **5 % du salaire annuel brut** ;
- le montant réellement attribué est distingué du montant potentiel.

Pour un salarié non éligible :

- `sport_bonus_eligible = false` ;
- le montant attribué est de **0 €**.

---

## 2. Journées bien-être

Les journées bien-être sont calculées à partir des activités sportives réalisées au cours des **12 derniers mois**.

Un salarié est éligible lorsqu'il atteint au moins :

**15 activités sportives sur les 12 derniers mois.**

Pour un salarié éligible :

```text
wellbeing_days_eligible = true
wellbeing_days_awarded = 5
```

Dans le cas contraire :

```text
wellbeing_days_eligible = false
wellbeing_days_awarded = 0
```

La fenêtre de calcul est dynamique et repose sur la date d'exécution du traitement.

---

## 3. Table métier `employee_benefits`

La table Delta `employee_benefits` contient une ligne par salarié.

Elle centralise notamment :

- l'identité du salarié ;
- la Business Unit ;
- le salaire brut ;
- le mode de déplacement ;
- la distance du trajet domicile-travail ;
- la validation du trajet sportif ;
- le nombre d'activités sportives ;
- la distance sportive cumulée ;
- l'éligibilité à la prime sportive ;
- le montant potentiel de la prime ;
- le montant réellement attribué ;
- l'éligibilité aux journées bien-être ;
- le nombre de journées attribuées ;
- la date de calcul.

Cette table constitue la principale sortie métier destinée à la restitution Power BI.

---

# Validation des trajets domicile-travail

Google Maps est utilisé pour contrôler la cohérence des trajets sportifs déclarés.

Les règles appliquées dans le POC sont :

| Mode | Mode Google Maps | Distance maximale |
|---|---|---:|
| Marche / Running | WALK | 15 km |
| Vélo / Trottinette / Autres | BICYCLE | 25 km |

Les résultats sont enregistrés dans la table :

```text
commute_validation
```

Dans le POC final :

- **68 salariés contrôlés** ;
- **68 trajets valides** ;
- **0 anomalie** ;
- **0 erreur API**.

---

# Simulation des activités sportives

Le POC ne se connecte pas directement à Strava.

Un simulateur Python permet donc de générer des activités sportives réalistes sur un historique de **365 jours**.

Les données générées comprennent notamment :

- l'identifiant de l'activité ;
- l'identifiant du salarié ;
- le type de sport ;
- la date et l'heure de début ;
- la date et l'heure de fin ;
- la distance lorsqu'elle est pertinente ;
- un commentaire éventuel.

Certaines activités peuvent légitimement avoir une distance NULL lorsque cette information n'est pas pertinente pour le sport concerné.

---

# CDC et traitement temps réel

## Debezium

Debezium utilise le **Change Data Capture** pour surveiller :

```text
public.activities
```

Lorsqu'une ligne est ajoutée ou modifiée, Debezium génère un événement contenant notamment les données avant et après modification.

Le connecteur utilise PostgreSQL avec `pgoutput`.

---

## Redpanda

Redpanda sert de plateforme de streaming entre PostgreSQL et les consommateurs.

Le topic CDC principal utilisé par le projet est :

```text
sport-data.public.activities
```

Les événements générés par Debezium y sont publiés automatiquement.

---

# Notifications Slack

La première branche du pipeline permet d'envoyer une notification lorsqu'une nouvelle activité est détectée.

```text
PostgreSQL
    ↓
Debezium
    ↓
Redpanda
    ↓
src/09_consume_activities.py
    ↓
Slack
```

Le consumer Python récupère l'événement CDC, extrait les informations utiles et prépare un message lisible.

Le Webhook Slack n'est jamais stocké directement dans le code.

Il est récupéré depuis la variable :

```text
SLACK_WEBHOOK_URL
```

définie dans le fichier local `.env`.

---

# Traitement Spark et Delta Lake

La seconde branche du pipeline est destinée à la partie analytique.

```text
Redpanda
    ↓
Spark / PySpark
    ↓
Contrôles
    ↓
Transformations
    ↓
Delta Lake
```

Spark permet notamment :

- de lire les événements ;
- de parser les données Debezium ;
- de contrôler leur qualité ;
- de transformer les données ;
- d'enrichir les activités ;
- de construire les données métier.

Delta Lake constitue la couche de stockage analytique du POC.

---

# Monitoring et qualité des données

Le pipeline contient plusieurs contrôles automatiques.

Ils portent notamment sur :

- les volumes PostgreSQL / Delta Lake ;
- les doublons `activity_id` ;
- les `activity_id` NULL ;
- les `employee_id` NULL ;
- les dates incohérentes ;
- les distances négatives ;
- les doublons `employee_id` dans `employee_benefits`.

Le monitoring final a confirmé :

| Contrôle | Résultat |
|---|---:|
| Salariés PostgreSQL | 161 |
| Activités PostgreSQL | 2 550 |
| Activités Delta Lake | 2 550 |
| Écart PostgreSQL / Delta | 0 |
| Doublons `activity_id` | 0 |
| `activity_id` NULL | 0 |
| `employee_id` NULL | 0 |
| Dates incohérentes | 0 |
| Distances négatives | 0 |
| Doublons `employee_id` dans `employee_benefits` | 0 |

État global obtenu :

```text
ÉTAT GLOBAL DU PIPELINE : OK
```

---

# Résultats métier obtenus

| Indicateur | Résultat |
|---|---:|
| Salariés traités | 161 |
| Activités sportives | 2 550 |
| Salariés avec au moins une activité sur les 12 derniers mois | 95 |
| Salariés éligibles à la prime sportive | 68 |
| Salariés non éligibles à la prime sportive | 93 |
| Montant total des primes sportives | 172 482,50 € |
| Salariés éligibles aux journées bien-être | 73 |
| Salariés non éligibles aux journées bien-être | 88 |
| Journées bien-être attribuées | 365 |

---

# Restitution Power BI

Les données métier sont exportées automatiquement au format CSV.

Deux fichiers principaux sont utilisés :

```text
data/powerbi/employee_benefits.csv
data/powerbi/activities.csv
```

Volumes finaux :

```text
employee_benefits.csv : 161 lignes
activities.csv         : 2 550 lignes
```

Le fichier :

```text
Sport_data.pbix
```

contient le tableau de bord Power BI du projet.

Il permet notamment de visualiser :

- le nombre total de salariés ;
- les salariés éligibles à la prime sportive ;
- le montant total des primes ;
- les salariés éligibles aux journées bien-être ;
- le nombre de journées attribuées ;
- les activités par type de sport ;
- les avantages par Business Unit.

Les règles métier sont calculées **en amont du dashboard**. Power BI est principalement utilisé comme couche de restitution et d'analyse.

---

# Installation et configuration

## Prérequis

Pour exécuter le projet localement :

- Git ;
- Docker Desktop ;
- Python ;
- PowerShell ou terminal équivalent ;
- Power BI Desktop pour consulter le dashboard.

---

## 1. Cloner le projet

```powershell
git clone https://github.com/Fatima012020/Creez_et_automatisez_une_architecture_de_donnees_THIAM_Fatou.git
cd Creez_et_automatisez_une_architecture_de_donnees_THIAM_Fatou
```

---

## 2. Créer l'environnement virtuel Python

```powershell
python -m venv .venv
```

Sous PowerShell :

```powershell
.\.venv\Scripts\Activate.ps1
```

Installer ensuite les dépendances :

```powershell
pip install -r requirements.txt
```

---

## 3. Configurer les variables d'environnement

Le fichier `.env` contient les informations sensibles nécessaires au projet.

Il n'est **jamais versionné**.

Créer un fichier :

```text
.env
```

à partir du fichier :

```text
.env.example
```

Variables principales :

```env
POSTGRES_PASSWORD=VOTRE_MOT_DE_PASSE
GOOGLE_MAPS_API_KEY=VOTRE_CLE_API
SLACK_WEBHOOK_URL=VOTRE_WEBHOOK_SLACK
```

Ne jamais renseigner de véritables secrets dans `.env.example`.

---

## 4. Démarrer l'infrastructure Docker

```powershell
docker compose up -d
```

Vérifier les services :

```powershell
docker compose ps
```

L'environnement Docker fournit notamment :

- PostgreSQL ;
- Debezium / Kafka Connect ;
- Redpanda ;
- Spark.

---

# Configuration Debezium

Le fichier :

```text
debezium/postgres-activities-connector.example.json
```

documente la configuration attendue pour le connecteur CDC.

Le fichier local contenant les véritables identifiants PostgreSQL est volontairement exclu du dépôt Git.

Le connecteur surveille :

```text
public.activities
```

et publie les changements dans :

```text
sport-data.public.activities
```

---

# Exécution du pipeline

## 1. Démarrer les services

```powershell
docker compose up -d
docker compose ps
```

---

## 2. Auditer les données

```powershell
python .\src\01_audit_donnees.py
```

---

## 3. Charger les données RH dans PostgreSQL

```powershell
python .\src\03_ingest_excel.py
```

---

## 4. Tester Google Maps sur un trajet

```powershell
python .\src\04_test_google_routes.py
```

---

## 5. Valider les trajets domicile-travail

```powershell
python .\src\05_validate_commutes.py
```

---

## 6. Générer les activités sportives

```powershell
python .\src\06_generate_activities.py
```

---

## 7. Charger les activités

```powershell
python .\src\07_load_activities.py
```

---

## 8. Contrôler leur qualité

```powershell
python .\src\08_check_activities_quality.py
```

---

# Exécution de la branche Slack

Le consumer Python écoute les événements publiés dans Redpanda.

```powershell
python .\src\09_consume_activities.py
```

Lorsqu'une nouvelle activité est détectée, une notification peut être envoyée dans Slack.

Un test spécifique Slack est également disponible :

```powershell
python .\src\10_test_slack.py
```

---

# Exécution de la branche Spark / Delta Lake

## Construire `employee_benefits`

```powershell
docker exec -it sport_data_spark /opt/spark/bin/spark-submit /opt/spark/work-dir/scripts/10_build_employee_benefits.py
```

---

## Exporter les données Power BI

```powershell
docker exec -it sport_data_spark /opt/spark/bin/spark-submit /opt/spark/work-dir/scripts/11_export_powerbi.py
```

Les fichiers produits sont :

```text
data/powerbi/employee_benefits.csv
data/powerbi/activities.csv
```

---

## Contrôler le pipeline

```powershell
docker exec -it sport_data_spark /opt/spark/bin/spark-submit /opt/spark/work-dir/scripts/12_monitor_pipeline.py
```

---

## Vérifier les dernières données Delta

```powershell
docker exec -it sport_data_spark /opt/spark/bin/spark-submit /opt/spark/work-dir/scripts/13_check_last_delta.py
```

---

## Nettoyage Delta

Le script :

```text
spark/14_cleanup_delta.py
```

permet d'effectuer le nettoyage prévu pour l'environnement Delta utilisé par le POC.

---

# Démonstration du flux temps réel

Le scénario de démonstration permet de montrer le parcours d'une nouvelle activité dans l'architecture.

```text
INSERT / UPDATE PostgreSQL
          │
          ▼
       Debezium
          │
          ▼
       Redpanda
       /      \
      /        \
Consumer       Spark
 Python          │
    │         Delta Lake
    ▼             │
  Slack     employee_benefits
                  │
                 CSV
                  │
               Power BI
```

La démonstration permet notamment de vérifier :

1. l'insertion d'une activité dans PostgreSQL ;
2. sa détection par Debezium ;
3. sa publication dans Redpanda ;
4. sa consommation ;
5. la notification Slack ;
6. son traitement dans la branche analytique ;
7. son stockage dans Delta Lake ;
8. la construction des données métier ;
9. l'export CSV ;
10. la restitution dans Power BI.

---

# Sécurité et gestion des secrets

Les informations sensibles sont stockées uniquement dans le fichier local :

```text
.env
```

Le `.gitignore` exclut notamment :

```text
.env
.env.*
.venv/
venv/
__pycache__/
data/checkpoints/
data/delta/
logs/
*.log
~$*
```

Les secrets concernés sont notamment :

- le mot de passe PostgreSQL ;
- la clé Google Maps API ;
- le Webhook Slack.

Le code récupère ces valeurs à partir des variables d'environnement.

Par exemple, le Webhook Slack est chargé avec :

```python
SLACK_WEBHOOK_URL = os.getenv("SLACK_WEBHOOK_URL")
```

Aucun Webhook Slack réel, token ou clé Google Maps réelle ne doit être enregistré directement dans le code source.

Le fichier :

```text
.env.example
```

documente uniquement les noms des variables nécessaires.

De la même manière, le fichier réel de configuration Debezium contenant les identifiants PostgreSQL n'est pas versionné.

Seul :

```text
debezium/postgres-activities-connector.example.json
```

est destiné au dépôt GitHub.

Les données Delta Lake et les checkpoints de streaming sont également exclus du dépôt.

---

# Livrables

Le dépôt contient les principaux livrables du projet.

### Code et architecture

- scripts Python ;
- scripts PySpark ;
- scripts SQL ;
- configuration Docker Compose ;
- exemple de configuration Debezium ;
- configuration des dépendances ;
- documentation du projet.

### Power BI

```text
Sport_data.pbix
```

Dashboard final de restitution métier.

### Support de soutenance PDF

```text
Support_Soutenance_P12_Sport_Data_Solution_Fatou_THIAM.pdf
```

### Support de soutenance PowerPoint

```text
Support_Soutenance_P12_Sport_Data_Solution_Fatou_THIAM.pptx
```

---

# Limites du POC

La solution actuelle constitue un POC fonctionnel.

Les principales limites sont :

- les activités Strava sont simulées ;
- l'environnement fonctionne localement avec Docker ;
- certaines règles métier sont directement définies dans le code ;
- le système n'est pas encore déployé dans un environnement cloud de production.

---

# Évolutions possibles

Les évolutions envisagées sont notamment :

- connecter directement l'API Strava ;
- externaliser les paramètres et seuils métier ;
- mettre en place une gestion centralisée des secrets ;
- renforcer le monitoring et l'alerting ;
- automatiser davantage l'orchestration ;
- déployer l'architecture dans le cloud ;
- renforcer la sécurité et la supervision de l'environnement.

---

# Conclusion

Ce projet met en œuvre une architecture de données complète allant de la source jusqu'à la restitution métier.

Il permet de mettre en pratique :

- l'audit et la qualité des données ;
- PostgreSQL ;
- le Change Data Capture avec Debezium ;
- le streaming avec Redpanda ;
- le traitement Python et PySpark ;
- le stockage Delta Lake ;
- les notifications Slack ;
- les règles métier ;
- le monitoring ;
- l'export de données ;
- la restitution Power BI ;
- Docker ;
- Git et GitHub.

Le POC permet ainsi de suivre le parcours d'une donnée depuis son arrivée dans PostgreSQL jusqu'à son exploitation métier dans Power BI, tout en proposant une branche temps réel dédiée aux notifications Slack.