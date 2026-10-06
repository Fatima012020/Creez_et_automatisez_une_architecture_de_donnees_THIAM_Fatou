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

L'architecture repose sur PostgreSQL, un flux temps réel basé sur le **CDC (Change Data Capture)** et deux branches de traitement après Redpanda.

Une fois les services démarrés avec Docker Compose, les composants du pipeline fonctionnent en continu. L'arrivée d'une nouvelle activité dans PostgreSQL constitue l'événement déclencheur : aucun lancement manuel des scripts de traitement n'est nécessaire.

```text
                         PostgreSQL
                             │
                          Debezium
                             │
                          Redpanda
                         /        \
                        /          \
              Consumer Python    Spark Streaming
                    │                │
                  Slack          Delta Lake
                                     │
                              Worker automatique
                                     │
                              employee_benefits
                                     │
                           CSV Power BI régénérés
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

Cette branche permet de détecter automatiquement une nouvelle activité sportive et d'envoyer une notification Slack.

### Branche analytique

```text
PostgreSQL
    │
Debezium
    │
Redpanda
    │
Spark Streaming
    │
Delta Lake
    │
Worker automatique
    │
employee_benefits
    │
CSV Power BI
    │
Power BI
```

Cette branche fonctionne automatiquement après l'arrivée d'une nouvelle activité.

Spark Streaming consomme les événements Redpanda et alimente Delta Lake. Le worker métier surveille ensuite les nouvelles versions Delta. Lorsqu'une nouvelle version est détectée, il déclenche automatiquement le recalcul de `employee_benefits` puis la régénération des fichiers CSV destinés à Power BI.

Power BI constitue la couche de restitution : les règles métier sont calculées en amont du dashboard.

### Services complémentaires

- **Google Maps API** : validation des trajets sportifs domicile-travail ;
- **Monitoring** : contrôle des volumes, doublons, valeurs NULL et incohérences ;
- **Docker Compose** : démarrage et maintien des différents services de l'architecture ;
- **Worker métier automatique** : détection des nouvelles versions Delta, recalcul des avantages salariés et export Power BI ;
- **Checkpoints** : mémorisation de l'état du streaming et de la dernière version métier traitée avec succès ;
- **Restart policies Docker** : redémarrage automatique des services concernés en cas d'arrêt inattendu.

---

## Fonctionnement du pipeline

Le pipeline est conçu pour fonctionner automatiquement après le démarrage des services.

1. Les données RH sont auditées puis chargées dans PostgreSQL lors de l'initialisation du POC.
2. Des activités sportives sont générées afin de simuler une source de données de type Strava.
3. Les trajets domicile-travail éligibles sont contrôlés avec Google Maps.
4. Debezium surveille en continu la table `public.activities` grâce au CDC.
5. Lorsqu'une nouvelle activité est ajoutée ou modifiée, Debezium publie automatiquement l'événement dans Redpanda.
6. Redpanda distribue l'événement vers deux branches fonctionnant en continu :
   - le consumer Python traite l'événement et envoie la notification Slack ;
   - Spark Streaming traite l'événement et alimente Delta Lake.
7. Le worker métier surveille automatiquement les nouvelles versions de Delta Lake.
8. Lorsqu'une nouvelle version est détectée, le worker recalcule `employee_benefits`.
9. Si le calcul réussit, les fichiers CSV destinés à Power BI sont automatiquement régénérés.
10. Le checkpoint métier est mis à jour uniquement lorsque l'ensemble du traitement a réussi.
11. Power BI utilise les CSV produits par le pipeline comme couche de restitution.

Ainsi, après l'arrivée d'une nouvelle activité, aucun script de traitement n'a besoin d'être lancé manuellement.

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
│   ├── 14_cleanup_delta.py
│   └── 15_auto_benefits_worker.py
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

# Automatisation du pipeline

L'architecture a été conçue pour éviter le lancement manuel des différentes étapes après l'arrivée d'une nouvelle activité.

Après :

```powershell
docker compose up -d
```

les services persistants restent actifs et surveillent les nouvelles données.

```text
Nouvelle activité PostgreSQL
            │
            ▼
         Debezium
            │
            ▼
         Redpanda
        /        \
       ▼          ▼
Consumer        Spark Streaming
   │               │
   ▼               ▼
 Slack          Delta Lake
                    │
                    ▼
             Worker automatique
                    │
                    ▼
             employee_benefits
                    │
                    ▼
              CSV Power BI
```

Une nouvelle activité constitue donc le seul événement nécessaire pour déclencher le traitement de bout en bout. Le consumer Slack, Spark Streaming et le worker métier fonctionnent comme des services persistants de l'architecture Docker.

---

# Résilience et reprise automatique

Le pipeline intègre des mécanismes permettant de reprendre automatiquement les traitements après certains incidents.

Spark Streaming utilise un checkpoint pour mémoriser l'état du flux. Le worker conserve également la dernière version Delta traitée avec succès dans un checkpoint persistant.

```text
Nouvelle version Delta
        │
        ▼
Calcul employee_benefits
        │
        ▼
Export Power BI
        │
        ├── Succès → checkpoint mis à jour
        └── Échec  → checkpoint conservé
                         │
                         ▼
                    Retry automatique
```

En cas d'échec du calcul métier ou de l'export :

- le checkpoint n'est pas avancé ;
- la version Delta reste à traiter ;
- le worker attend avant d'effectuer une nouvelle tentative ;
- après rétablissement du service indisponible, le traitement peut reprendre automatiquement.

Ce mécanisme a notamment été testé en rendant PostgreSQL temporairement indisponible pendant un traitement : le checkpoint n'a pas été avancé pendant l'échec et le worker a repris automatiquement le traitement après le retour de PostgreSQL.

Les services Docker concernés utilisent également une politique de redémarrage afin d'améliorer la résilience de l'environnement local.

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

Les données métier destinées à Power BI sont produites en amont par le pipeline.

Lorsqu'une nouvelle version Delta est détectée, le worker métier :

1. recalcule automatiquement `employee_benefits` ;
2. contrôle le résultat du traitement ;
3. régénère automatiquement les fichiers CSV destinés à Power BI.

Deux fichiers principaux sont utilisés :

```text
data/powerbi/employee_benefits.csv
data/powerbi/activities.csv
```

Le fichier `Sport_data.pbix` est connecté à ces données et constitue la couche de restitution du projet.

Les règles métier ne sont pas recalculées dans Power BI : elles sont préparées en amont par le pipeline. Dans l'environnement local du POC, une actualisation du rapport Power BI permet d'afficher les dernières données produites automatiquement.

Le tableau de bord permet notamment de visualiser :

- le nombre total de salariés ;
- les salariés éligibles à la prime sportive ;
- le montant total des primes ;
- les salariés éligibles aux journées bien-être ;
- le nombre de journées attribuées ;
- les activités par type de sport ;
- les avantages par Business Unit.

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

L'environnement Docker démarre notamment :

- PostgreSQL ;
- Debezium / Kafka Connect ;
- Redpanda ;
- Spark Streaming ;
- le consumer Python pour les notifications Slack ;
- le worker automatique chargé du recalcul métier et des exports Power BI.

Après le démarrage des services, aucune exécution manuelle des scripts de traitement n'est nécessaire pour traiter une nouvelle activité.

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

## Mode automatique — fonctionnement nominal

Démarrer l'ensemble des services :

```powershell
docker compose up -d
```

Vérifier leur état :

```powershell
docker compose ps
```

Une fois les services démarrés, le pipeline fonctionne en continu.

L'ajout d'une nouvelle activité dans `public.activities` constitue l'événement déclencheur. Il n'est pas nécessaire de lancer manuellement le consumer, Spark Streaming, le calcul de `employee_benefits` ou l'export Power BI.

```text
INSERT / nouvelle activité
          ↓
       Debezium
          ↓
       Redpanda
       /      \
      ↓        ↓
   Slack     Delta Lake
                ↓
         Worker automatique
                ↓
        employee_benefits
                ↓
           CSV Power BI
```

Les scripts individuels restent disponibles pour le développement, les tests et le diagnostic, mais ils ne constituent pas le mode normal d'exécution du pipeline.

---

# Branche Slack

Le consumer Python est exécuté comme un service persistant de l'environnement Docker. Il écoute en continu les événements publiés dans Redpanda et envoie automatiquement une notification Slack lorsqu'une nouvelle activité est détectée.

Il n'est donc pas nécessaire de lancer `src/09_consume_activities.py` manuellement lors du fonctionnement nominal.

---

# Branche Spark / Delta Lake

Spark Streaming fonctionne comme un service persistant et consomme automatiquement les nouvelles activités publiées dans Redpanda. Les activités valides sont enregistrées dans Delta Lake.

Le worker métier surveille ensuite les nouvelles versions Delta. Lorsqu'une nouvelle version est disponible, il exécute automatiquement :

```text
10_build_employee_benefits.py
        ↓
11_export_powerbi.py
```

Le premier traitement recalcule `employee_benefits` et le second régénère les fichiers `data/powerbi/employee_benefits.csv` et `data/powerbi/activities.csv`.

Ces scripts peuvent toujours être exécutés manuellement à des fins de développement ou de diagnostic, mais leur lancement manuel n'est pas nécessaire dans le fonctionnement nominal.

---

# Outils de contrôle et de diagnostic

Les scripts `12_monitor_pipeline.py`, `13_check_last_delta.py` et `14_cleanup_delta.py` sont des outils de vérification et de maintenance. Ils ne déclenchent pas le fonctionnement nominal du pipeline.

---

# Démonstration du pipeline automatisé

La démonstration a pour objectif de montrer qu'une seule nouvelle activité suffit à déclencher l'ensemble du pipeline.

```text
                  Nouvelle activité
                      PostgreSQL
                          │
                          ▼
                       Debezium
                          │
                          ▼
                       Redpanda
                      /        \
                     ▼          ▼
             Consumer Python   Spark Streaming
                    │              │
                    ▼              ▼
                  Slack         Delta Lake
                                   │
                                   ▼
                            Worker automatique
                                   │
                                   ▼
                           employee_benefits
                                   │
                                   ▼
                              CSV Power BI
```

Le scénario de démonstration est volontairement minimal :

1. vérifier que les services Docker sont démarrés ;
2. insérer une seule nouvelle activité dans PostgreSQL ;
3. ne lancer aucun script de traitement ;
4. observer l'apparition automatique de la notification Slack ;
5. constater le traitement automatique de la branche Spark / Delta Lake ;
6. constater le recalcul automatique de `employee_benefits` ;
7. constater la régénération des CSV ;
8. actualiser Power BI afin d'afficher les nouvelles données.

**Après l'INSERT PostgreSQL, aucun script de traitement n'est lancé manuellement.**

Les commandes de consultation des logs éventuellement utilisées pendant la démonstration servent uniquement à observer les traitements réalisés automatiquement.

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

La solution actuelle constitue un POC fonctionnel et automatisé dans un environnement Docker local.

Les principales limites sont :

- les activités Strava sont simulées ;
- l'environnement fonctionne localement avec Docker ;
- certaines règles métier sont directement définies dans le code ;
- Power BI Desktop nécessite une actualisation du rapport pour afficher les derniers CSV produits ;
- le système n'est pas encore déployé dans un environnement cloud de production ;
- l'orchestration repose sur les services Docker, le streaming et le worker métier plutôt que sur un orchestrateur de production dédié.

---

# Évolutions possibles

Les évolutions envisagées sont notamment :

- connecter directement l'API Strava ;
- externaliser les paramètres et seuils métier ;
- mettre en place une gestion centralisée des secrets ;
- renforcer le monitoring et l'alerting ;
- industrialiser l'orchestration avec un outil dédié dans un environnement de production ;
- automatiser l'actualisation de la couche de restitution dans un environnement Power BI Service ;
- déployer l'architecture dans le cloud ;
- renforcer la sécurité et la supervision de l'environnement.

---

# Conclusion

Ce projet met en œuvre une architecture de données événementielle permettant de traiter automatiquement une nouvelle activité sportive depuis PostgreSQL jusqu'aux données destinées à Power BI.

Le POC permet de mettre en pratique :

- l'audit et la qualité des données ;
- PostgreSQL ;
- le Change Data Capture avec Debezium ;
- le streaming avec Redpanda ;
- les traitements Python et PySpark ;
- Spark Streaming ;
- le stockage Delta Lake ;
- les notifications Slack automatiques ;
- le calcul des règles métier ;
- le monitoring ;
- les checkpoints et la reprise sur erreur ;
- l'export automatique des données Power BI ;
- Docker / Docker Compose ;
- Git et GitHub.

Après le démarrage de l'environnement, une nouvelle activité dans PostgreSQL suffit à déclencher automatiquement sa propagation dans le pipeline.

Debezium capture le changement, Redpanda distribue l'événement, le consumer Python alimente Slack, Spark Streaming alimente Delta Lake et le worker métier recalcule les avantages salariés puis régénère les fichiers destinés à Power BI.

Le POC démontre ainsi non seulement la faisabilité technique de l'architecture, mais également son **automatisation de bout en bout, son observabilité et sa capacité de reprise après incident**.
