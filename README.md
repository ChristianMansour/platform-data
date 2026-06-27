# Plateforme de Données Cloud — Christian MANSOUR

Projet réalisé dans le cadre de l'UE Plateforme de données sur le cloud.

Ce projet construit une plateforme de données complète : infrastructure Docker, orchestration Airflow, transformation dbt (médaillon Bronze/Silver/Gold), et intégration Snowflake via Terraform (Infrastructure as Code).

---

## Sommaire

1. [Architecture générale](#architecture-générale)
2. [Prérequis](#prérequis)
3. [Installation et mode opératoire](#installation-et-mode-opératoire)
4. [Infrastructure Docker](#infrastructure-docker)
5. [Orchestration Airflow](#orchestration-airflow)
6. [Transformation dbt](#transformation-dbt)
7. [Snowflake et Infrastructure as Code](#snowflake-et-infrastructure-as-code)
8. [Lac de données (Snowflake Stages)](#lac-de-données-snowflake-stages)
9. [Gouvernance et gestion des secrets](#gouvernance-et-gestion-des-secrets)
10. [Gestion des erreurs](#gestion-des-erreurs)
11. [Structure du projet](#structure-du-projet)

---

## Architecture générale

La plateforme suit une architecture **ELT** (Extract → Load → Transform) avec un modèle en médaillon **Bronze / Silver / Gold** :

```
Sources (DVF 2025, Open-Meteo)
        │
        ▼ EXTRACTION (Airflow)
        │
        ▼ CHARGEMENT BRONZE (données brutes)
        │
        ▼ TRANSFORMATION dbt
        │
   ┌────┴────┐
   ▼         ▼
 SILVER     (nettoyé, typé)
   │
   ▼
 GOLD       (marts, schéma en étoile)
```

Deux cibles sont supportées en parallèle :
- **PostgreSQL** (local, Docker) — pipeline `elt_e2e`
- **Snowflake** (cloud) — pipeline `elt_snowflake`

---

## Prérequis

- Docker et Docker Compose installés
- Accès VPN WireGuard actif (environnement de cours)
- Accès SSH à la VM
- Un compte Snowflake (trial ou payant)
- Terraform installé (`terraform --version`)
- Python 3.11+ avec `pip` (pour dbt en local si besoin)

---

## Installation et mode opératoire

### 1. Cloner le projet

```bash
git clone https://github.com/ChristianMansour/platform-data.git
cd platform-data
```

### 2. Créer le réseau Docker

```bash
docker network create platform-net
```

### 3. Créer les répertoires de volumes

```bash
mkdir -p /opt/docker-volumes/postgres
mkdir -p /opt/docker-volumes/redis
mkdir -p /opt/docker-volumes/pgadmin
mkdir -p /opt/docker-volumes/code-server
mkdir -p /opt/docker-volumes/airflow/dags
mkdir -p /opt/docker-volumes/airflow/logs
mkdir -p /opt/docker-volumes/airflow/config
mkdir -p /opt/docker-volumes/airflow/plugins
mkdir -p /opt/docker-volumes/airflow/data
sudo chown -R 5050:5050 /opt/docker-volumes/pgadmin
```

### 4. Lancer l'infrastructure de base (Postgres, Redis, pgAdmin, code-server)

```bash
docker compose up -d
docker ps
```

Vous devez voir 4 conteneurs `Up` : `postgres-warehouse`, `redis`, `pgadmin`, `code-server`.

### 5. Créer les bases et schémas Postgres

```bash
docker exec -it postgres-warehouse psql -U platform -d warehouse -c "
CREATE USER svc_airflow WITH PASSWORD 'airflow';
CREATE DATABASE airflow OWNER svc_airflow;
CREATE SCHEMA IF NOT EXISTS bronze;
CREATE SCHEMA IF NOT EXISTS silver;
CREATE SCHEMA IF NOT EXISTS gold;
GRANT ALL PRIVILEGES ON DATABASE warehouse TO svc_airflow;
GRANT ALL ON SCHEMA bronze TO svc_airflow;
GRANT ALL ON SCHEMA silver TO svc_airflow;
GRANT ALL ON SCHEMA gold TO svc_airflow;
"
```

### 6. Configurer et lancer Airflow

```bash
cd airflow
```

Créer le fichier `.env` (voir `.env.example` fourni) avec :
- une clé Fernet générée : `python3 -c "from cryptography.fernet import Fernet; print(Fernet.generate_key().decode())"`
- les identifiants de connexion à la base airflow (`svc_airflow` / `airflow`)

```bash
docker compose up airflow-init
docker compose up -d
docker ps
```

Interface Airflow : **http://10.1.1.1:8081** (login `airflow` / `airflow`)

### 7. Configurer les connexions Airflow

Dans l'UI Airflow → **Admin → Connections**, créer :

| Connection Id | Type | Host | Login | Password | Extra |
|---|---|---|---|---|---|
| `postgres_warehouse` | Postgres | `postgres` | `svc_airflow` | `airflow` | Database: `warehouse`, Port: `5432` |
| `my_git_conn` | Generic | URL du repo GitHub | Username GitHub | Personal Access Token | — |
| `snowflake_platform` | Snowflake | — | `svc_terraform` | mot de passe Snowflake | `{"account": "...", "warehouse": "PLATFORM_WH", "database": "PLATFORM_DB", "role": "ACCOUNTADMIN"}` |

### 8. Configurer dbt

```bash
cd dbt
cp profiles.yml.example profiles.yml
# Éditer profiles.yml avec vos vrais identifiants Postgres et Snowflake
sudo chmod -R 777 .
```

### 9. Déployer Snowflake via Terraform

```bash
cd terraform/snowflake
cp terraform.tfvars.example terraform.tfvars
# Éditer terraform.tfvars avec votre account Snowflake, user svc_terraform, password
terraform init
terraform plan
terraform apply
```

### 10. Lancer les pipelines

Dans l'UI Airflow, déclencher :
- `elt_e2e` : pipeline complet sur Postgres (extraction + chargement + dbt run + dbt test)
- `elt_snowflake` : pipeline complet sur Snowflake (extraction + PUT stage + COPY INTO + dbt run --target snowflake)

---

## Infrastructure Docker

### Services déployés

| Service | Image | Port | Rôle |
|---|---|---|---|
| `postgres-warehouse` | postgres:17 | 5432 | Entrepôt de données (warehouse + airflow) |
| `redis` | redis:7-alpine | 6379 | Broker Celery pour Airflow |
| `pgadmin` | dpage/pgadmin4 | 5050 | Interface d'administration PostgreSQL |
| `code-server` | codercom/code-server | 8080 | IDE web (VS Code dans le navigateur) |
| `airflow-apiserver` | apache/airflow:3.1.7 | 8081 | Interface web Airflow |
| `airflow-scheduler` | apache/airflow:3.1.7 | — | Planification des DAGs |
| `airflow-dag-processor` | apache/airflow:3.1.7 | — | Synchronisation des DAGs (GitDagBundle) |
| `airflow-worker` | apache/airflow:3.1.7 | — | Exécution des tâches (CeleryExecutor) |
| `airflow-triggerer` | apache/airflow:3.1.7 | — | Gestion des tâches asynchrones/deferred |

### Structure des données sur l'hôte

```
/opt/
├── docker/                          # Docker data-root (images, conteneurs)
├── docker-volumes/
│   ├── postgres/                    # Données PostgreSQL
│   ├── redis/                       # Données Redis
│   ├── pgadmin/                     # Configuration pgAdmin
│   ├── code-server/                 # Configuration VS Code Server
│   └── airflow/
│       ├── dags/                    # DAGs (montage local, en complément du GitDagBundle)
│       ├── logs/                    # Logs d'exécution Airflow
│       ├── config/                  # Configuration Airflow
│       ├── plugins/                 # Plugins Airflow
│       └── data/                    # Fichiers extraits (DVF, météo)
```

### Accès aux services

| Service | URL | Identifiants |
|---|---|---|
| pgAdmin | http://10.1.1.1:5050 | `admin@admin.com` / `admin` |
| Airflow | http://10.1.1.1:8081 | `airflow` / `airflow` |
| code-server | http://10.1.1.1:8080 | mot de passe `admin` |
| Postgres (warehouse) | `10.1.1.1:5432` ou `postgres` (réseau Docker) | `platform` / `platform` |

### Vérification de l'installation

```bash
docker ps
docker exec -it postgres-warehouse psql -U platform -d warehouse -c "SELECT 1 AS test;"
docker exec -it redis redis-cli PING
```

---

## Orchestration Airflow

### DAGs disponibles

| DAG | Rôle |
|---|---|
| `hello_world_simple` | DAG de test TaskFlow API (validation infra) |
| `dvf_2025_extraction` | Extraction DVF 2025 → Bronze → Silver (Postgres) |
| `openmeteo_extraction` | Extraction météo Open-Meteo → Bronze → Silver (Postgres) |
| `elt_e2e` | **Pipeline complet** Extract → Load → dbt run → dbt test (Postgres) |
| `elt_snowflake` | **Pipeline complet** Extract → PUT stage → COPY INTO → dbt run --target snowflake |

### Déploiement des DAGs : GitDagBundle

Les DAGs ne sont pas copiés manuellement dans les conteneurs : ils sont synchronisés automatiquement depuis le dépôt GitHub via un **GitDagBundle** Airflow 3.x (`airflow.providers.git.bundles.git.GitDagBundle`), configuré dans `airflow/.env` :

```
AIRFLOW__DAG_PROCESSOR__DAG_BUNDLE_CONFIG_LIST=[{"name":"my-git-bundle","classpath":"airflow.providers.git.bundles.git.GitDagBundle","kwargs":{"tracking_ref":"main","git_conn_id":"my_git_conn","subdir":"dags"}}]
```

Tout commit poussé sur la branche `main` (dossier `dags/`) est automatiquement repris par Airflow après resynchronisation du `dag-processor`.

### CI/CD

Un workflow GitHub Actions (`.github/workflows/ci.yml`) valide chaque push sur `dags/` :
- **Lint** des DAGs avec `ruff`
- **Test de parsing** (validité syntaxique de chaque fichier DAG)

### Pipeline elt_e2e (Postgres) — détail

```
extract_meteo ──┐
                ├──> load_meteo_bronze ──┐
extract_dvf ────┘                       ├──> run_dbt ──> test_dbt
                  load_dvf_bronze ───────┘
```

### Pipeline elt_snowflake — détail

```
extract_meteo ──> put_meteo_raw_stage ──> create_bronze_meteo ──> copy_meteo_bronze ──┐
                                                                                        ├──> run_dbt_snowflake
extract_dvf ────> put_dvf_raw_stage ────> create_bronze_dvf ────> copy_dvf_bronze ────┘
```

---

## Transformation dbt

### Modèles

| Couche | Modèle | Cible | Description |
|---|---|---|---|
| Silver | `meteo_quotidien` | Postgres | Météo nettoyée + colonne calculée `a_plu` |
| Silver | `dvf_mutations` | Postgres | DVF nettoyé, typé, filtré (valeur foncière > 0) |
| Silver | `meteo_quotidien_snowflake` | Snowflake | Équivalent météo, syntaxe Snowflake (`TRY_TO_DATE`, `TRY_TO_DOUBLE`) |
| Silver | `dvf_mutations_snowflake` | Snowflake | Équivalent DVF, syntaxe Snowflake (`TRY_TO_NUMBER`) |
| Gold | `dim_commune` | Postgres | Dimension commune (schéma en étoile DVF) |
| Gold | `dim_type_local` | Postgres | Dimension type de bien (schéma en étoile DVF) |
| Gold | `fact_mutations` | Postgres | Table de fait des mutations DVF |
| Gold | `mart_dvf_par_commune` | Postgres | Agrégat DVF par commune (nb mutations, prix m²) |
| Gold | `mart_meteo_quotidien` | Postgres | Agrégat météo quotidien |
| Gold | `mart_meteo_snowflake` | Snowflake | Agrégat météo + indicateur `is_rainy` |
| Gold | `mart_dvf_par_commune_snowflake` | Snowflake | Agrégat DVF par commune (Snowflake) |

### Sources

Déclarées dans `dbt/models/sources.yml` : `bronze.dvf_mutations`, `bronze.meteo_quotidien`.

### Tests

11 tests dbt (`unique`, `not_null`) répartis sur les modèles silver et gold, déclarés dans `dbt/models/schema.yml`. Tous passent (`PASS=11`).

### Lancer dbt manuellement

```bash
cd dbt
# Cible Postgres
dbt run --exclude meteo_quotidien_snowflake mart_meteo_snowflake
dbt test --exclude meteo_quotidien_snowflake mart_meteo_snowflake

# Cible Snowflake
dbt run --target snowflake --select meteo_quotidien_snowflake mart_meteo_snowflake dvf_mutations_snowflake mart_dvf_par_commune_snowflake
```

### Documentation dbt

```bash
dbt docs generate --exclude meteo_quotidien_snowflake mart_meteo_snowflake
dbt docs serve --port 8082 --host 0.0.0.0
```
Puis ouvrir **http://10.1.1.1:8082** (lineage graph, descriptions des modèles et colonnes).

---

## Snowflake et Infrastructure as Code

### Ressources déployées via Terraform (`terraform/snowflake/`)

| Ressource | Nom | Description |
|---|---|---|
| Warehouse | `PLATFORM_WH` | Compute X-SMALL, auto-suspend 60s |
| Database | `PLATFORM_DB` | Base de données principale |
| Schema | `BRONZE`, `SILVER`, `GOLD` | Médaillon |
| Stage | `RAW_STAGE` | Lac de données — fichiers bruts |
| Stage | `REFINED_STAGE` | Lac de données — zone raffinée (réservée) |
| Role | `ROLE_ENGINEER` | Tous droits sur Bronze/Silver/Gold |
| Role | `ROLE_ANALYST` | Lecture (SELECT) sur Gold uniquement |

### Modèle RBAC : utilisateur, rôle, privilège

- **Utilisateur** : identité qui se connecte à Snowflake (ex. `svc_terraform`). Un utilisateur n'a par défaut aucun droit.
- **Rôle** : conteneur de privilèges, assigné à un ou plusieurs utilisateurs. Snowflake fonctionne uniquement par rôles (jamais de droit direct à un utilisateur).
- **Privilège** : permission précise sur un objet (ex. `USAGE` sur un warehouse, `SELECT` sur une table, `ALL PRIVILEGES` sur un schéma).

### Rôles créés et justification

| Rôle | Privilèges accordés | Justification |
|---|---|---|
| `ROLE_ENGINEER` | `USAGE` sur warehouse et database, `ALL PRIVILEGES` sur Bronze/Silver/Gold, `READ`/`WRITE` sur les stages | Représente l'équipe data engineering : doit pouvoir créer, charger et transformer les données dans toutes les couches du médaillon, ainsi que déposer/lire des fichiers dans le lac de données. |
| `ROLE_ANALYST` | `USAGE` sur warehouse, database et schéma Gold, `SELECT` sur les tables Gold uniquement | Représente un analyste métier : n'a besoin que de consulter les données déjà préparées (marts), sans accès aux données brutes (Bronze) ni à la logique de transformation (Silver). Principe du moindre privilège. |

Cette séparation illustre le principe de **moindre privilège** : chaque rôle reçoit uniquement les droits nécessaires à sa fonction, limitant la surface d'erreur et les risques de sécurité.

### Déploiement

```bash
cd terraform/snowflake
terraform init
terraform plan
terraform apply
```

20 ressources créées (`Apply complete! Resources: 20 added, 0 changed, 0 destroyed.`).

---

## Lac de données (Snowflake Stages)

### Localisation

Le lac de données est implémenté via des **stages internes Snowflake** :
- `PLATFORM_DB.BRONZE.RAW_STAGE` : zone brute (fichiers sources tels que reçus)
- `PLATFORM_DB.BRONZE.REFINED_STAGE` : zone raffinée (réservée, évolutions futures type Parquet)

### Convention de partitionnement

Les fichiers sont déposés dans `RAW_STAGE` avec une structure hiérarchique par clé, facilitant les lectures ciblées :

```
@RAW_STAGE/
├── dvf/
│   └── annee=2025/
│       └── dvf_2025.csv.gz
└── meteo/
    └── date=2026-06-27/
        └── meteo_2026-06-27.json
```

### Flux d'alimentation

1. **Extraction** : Airflow récupère les données (API Open-Meteo, fichier DVF data.gouv.fr) et les écrit localement sur le worker.
2. **PUT** : une tâche Airflow (`snowflake.connector`) dépose le fichier brut dans `RAW_STAGE` via la commande SQL `PUT`.
3. **COPY INTO** : un opérateur SQL Airflow (`SQLExecuteQueryOperator`) charge les fichiers du stage vers les tables Bronze (`BRONZE.DVF_MUTATIONS`, `BRONZE.METEO_QUOTIDIEN`).
4. **dbt run --target snowflake** : transforme Bronze → Silver → Gold avec la syntaxe SQL Snowflake.

### Vérification

```sql
USE DATABASE PLATFORM_DB;
USE SCHEMA BRONZE;
LIST @RAW_STAGE;
SELECT COUNT(*) FROM BRONZE.DVF_MUTATIONS;
SELECT COUNT(*) FROM BRONZE.METEO_QUOTIDIEN;
```

---

## Gouvernance et gestion des secrets

Aucun mot de passe, token ou clé API n'est versionné en clair dans le code.

### Connexions et Variables Airflow (Admin → Connections)

| Connection ID | Type | Usage |
|---|---|---|
| `postgres_warehouse` | Postgres | Accès à la base `warehouse` depuis les DAGs (Postgres) |
| `snowflake_platform` | Snowflake | Accès à Snowflake depuis les DAGs |
| `my_git_conn` | Generic | Authentification du GitDagBundle |

Dans le code des DAGs, les credentials sont récupérés via :

```python
from airflow.hooks.base import BaseHook
conn = BaseHook.get_connection("postgres_warehouse")
engine = create_engine(f"postgresql://{conn.login}:{conn.password}@{conn.host}:{conn.port}/{conn.schema}")
```

### Fichiers exclus du versioning (`.gitignore`)

| Fichier réel (non versionné) | Modèle fourni (versionné) |
|---|---|
| `airflow/.env` | `airflow/.env.example` |
| `dbt/profiles.yml` | `dbt/profiles.yml.example` |
| `terraform/snowflake/terraform.tfvars` | `terraform/snowflake/terraform.tfvars.example` |
| `*.tfstate*`, `.terraform/` | — |

---

## Gestion des erreurs

- **Retries** : les tâches d'extraction (`extract_meteo`, `extract_dvf`) sont configurées avec `retries=3` et `retry_delay` exponentiel (`retry_exponential_backoff=True`), pour absorber les erreurs réseau transitoires.
- **Notification d'échec** : un callback `on_failure_callback` envoie une notification vers **ntfy.sh** (topic `airflow-platform-data-notifications`) en cas d'échec d'une tâche, avec le `dag_id`, `task_id`, `run_id` et l'horodatage.
- **ON_ERROR = 'CONTINUE'** sur les `COPY INTO` Snowflake : permet de ne pas bloquer un chargement bronze sur quelques lignes malformées.

---

## Structure du projet

```
platform-data/
├── README.md                     # Ce fichier
├── docker-compose.yml            # Infra Postgres/Redis/pgAdmin/code-server
├── .gitignore
├── dags/                         # DAGs Airflow (synchronisés via GitDagBundle)
│   ├── hello_dag.py
│   ├── dvf_extraction.py
│   ├── openmeteo_extraction.py
│   ├── elt_e2e.py
│   └── elt_snowflake.py
├── airflow/
│   ├── docker-compose.yaml       # Compose Airflow (CeleryExecutor)
│   └── .env.example              # Modèle de configuration (sans secrets)
├── dbt/                          # Projet dbt
│   ├── dbt_project.yml
│   ├── profiles.yml.example      # Modèle de configuration (sans secrets)
│   ├── models/
│   │   ├── sources.yml
│   │   ├── schema.yml            # Tests et descriptions
│   │   ├── silver/
│   │   └── gold/
│   └── ...
├── terraform/
│   └── snowflake/
│       ├── versions.tf
│       ├── variables.tf
│       ├── main.tf
│       └── terraform.tfvars.example
└── .github/
    └── workflows/
        └── ci.yml                # CI : lint + parsing des DAGs
```

---

## Auteur

**Christian MANSOUR** — UE Plateforme de données sur le cloud
