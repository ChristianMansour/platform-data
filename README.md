# Plateforme de Données Cloud — Christian MANSOUR

Projet réalisé dans le cadre de l'UE Plateforme de données sur le cloud.

Ce projet construit une plateforme de données complète : infrastructure Docker, orchestration Airflow, transformation dbt (medaillon Bronze/Silver/Gold), et integration Snowflake via Terraform (Infrastructure as Code).

---

## Sommaire

1. Architecture generale
2. Prerequis
3. Installation et mode operatoire
4. Infrastructure Docker
5. Orchestration Airflow
6. Transformation dbt
7. Snowflake et Infrastructure as Code
8. Lac de donnees (Snowflake Stages)
9. Gouvernance et gestion des secrets
10. Gestion des erreurs
11. Structure du projet

---

## Architecture generale

La plateforme suit une architecture ELT (Extract -> Load -> Transform) avec un modele en medaillon Bronze / Silver / Gold :

Sources (DVF 2025, Open-Meteo)
        |
        v EXTRACTION (Airflow)
        |
        v CHARGEMENT BRONZE (donnees brutes)
        |
        v TRANSFORMATION dbt
        |
   SILVER (nettoye, type)
        |
        v
   GOLD (marts, schema en etoile)

Deux cibles sont supportees en parallele :
- PostgreSQL (local, Docker) -- pipeline elt_e2e
- Snowflake (cloud) -- pipeline elt_snowflake

---

## Prerequis

- Docker et Docker Compose installes
- Acces VPN WireGuard actif (environnement de cours)
- Acces SSH a la VM
- Un compte Snowflake (trial ou payant)
- Terraform installe (terraform --version)
- Python 3.11+ avec pip (pour dbt en local si besoin)

---

## Installation et mode operatoire

### 1. Cloner le projet

git clone https://github.com/ChristianMansour/platform-data.git
cd platform-data

### 2. Creer le reseau Docker

docker network create platform-net

### 3. Creer les repertoires de volumes

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

### 4. Lancer l'infrastructure de base (Postgres, Redis, pgAdmin, code-server)

docker compose up -d
docker ps

Vous devez voir 4 conteneurs Up : postgres-warehouse, redis, pgadmin, code-server.

### 5. Creer les bases et schemas Postgres

docker exec -it postgres-warehouse psql -U platform -d warehouse -c "CREATE USER svc_airflow WITH PASSWORD 'airflow'; CREATE DATABASE airflow OWNER svc_airflow; CREATE SCHEMA IF NOT EXISTS bronze; CREATE SCHEMA IF NOT EXISTS silver; CREATE SCHEMA IF NOT EXISTS gold; GRANT ALL PRIVILEGES ON DATABASE warehouse TO svc_airflow; GRANT ALL ON SCHEMA bronze TO svc_airflow; GRANT ALL ON SCHEMA silver TO svc_airflow; GRANT ALL ON SCHEMA gold TO svc_airflow;"

### 6. Configurer et lancer Airflow

cd airflow

Creer le fichier .env (voir .env.example fourni) avec :
- une cle Fernet generee : python3 -c "from cryptography.fernet import Fernet; print(Fernet.generate_key().decode())"
- les identifiants de connexion a la base airflow (svc_airflow / airflow)

docker compose up airflow-init
docker compose up -d
docker ps

Interface Airflow : http://10.1.1.1:8081 (login airflow / airflow)

### 7. Configurer les connexions Airflow

Dans l'UI Airflow -> Admin -> Connections, creer :

- postgres_warehouse : type Postgres, host postgres, login svc_airflow, password airflow, database warehouse, port 5432
- my_git_conn : type Generic, host URL du repo GitHub, login username GitHub, password Personal Access Token
- snowflake_platform : type Snowflake, login svc_terraform, password mot de passe Snowflake, Extra JSON avec account, warehouse PLATFORM_WH, database PLATFORM_DB, role ACCOUNTADMIN

### 8. Configurer dbt

cd dbt
cp profiles.yml.example profiles.yml

Editer profiles.yml avec vos vrais identifiants Postgres et Snowflake

sudo chmod -R 777 .

### 9. Deployer Snowflake via Terraform

cd terraform/snowflake
cp terraform.tfvars.example terraform.tfvars

Editer terraform.tfvars avec votre account Snowflake, user svc_terraform, password

terraform init
terraform plan
terraform apply

### 10. Lancer les pipelines

Dans l'UI Airflow, declencher :
- elt_e2e : pipeline complet sur Postgres (extraction + chargement + dbt run + dbt test)
- elt_snowflake : pipeline complet sur Snowflake (extraction + PUT stage + COPY INTO + dbt run --target snowflake)

---

## Infrastructure Docker

### Services deployes

- postgres-warehouse (postgres:17, port 5432) : Entrepot de donnees (warehouse + airflow)
- redis (redis:7-alpine, port 6379) : Broker Celery pour Airflow
- pgadmin (dpage/pgadmin4, port 5050) : Interface d'administration PostgreSQL
- code-server (codercom/code-server, port 8080) : IDE web (VS Code dans le navigateur)
- airflow-apiserver (apache/airflow:3.1.7, port 8081) : Interface web Airflow
- airflow-scheduler (apache/airflow:3.1.7) : Planification des DAGs
- airflow-dag-processor (apache/airflow:3.1.7) : Synchronisation des DAGs (GitDagBundle)
- airflow-worker (apache/airflow:3.1.7) : Execution des taches (CeleryExecutor)
- airflow-triggerer (apache/airflow:3.1.7) : Gestion des taches asynchrones/deferred

### Structure des donnees sur l'hote

/opt/
- docker/ : Docker data-root (images, conteneurs)
- docker-volumes/
  - postgres/ : Donnees PostgreSQL
  - redis/ : Donnees Redis
  - pgadmin/ : Configuration pgAdmin
  - code-server/ : Configuration VS Code Server
  - airflow/
    - dags/ : DAGs (montage local, en complement du GitDagBundle)
    - logs/ : Logs d'execution Airflow
    - config/ : Configuration Airflow
    - plugins/ : Plugins Airflow
    - data/ : Fichiers extraits (DVF, meteo)

### Acces aux services

- pgAdmin : http://10.1.1.1:5050 -- admin@admin.com / admin
- Airflow : http://10.1.1.1:8081 -- airflow / airflow
- code-server : http://10.1.1.1:8080 -- mot de passe admin
- Postgres (warehouse) : 10.1.1.1:5432 ou postgres (reseau Docker) -- platform / platform

### Verification de l'installation

docker ps
docker exec -it postgres-warehouse psql -U platform -d warehouse -c "SELECT 1 AS test;"
docker exec -it redis redis-cli PING

---

## Orchestration Airflow

### DAGs disponibles

- hello_world_simple : DAG de test TaskFlow API (validation infra)
- dvf_2025_extraction : Extraction DVF 2025 -> Bronze -> Silver (Postgres)
- openmeteo_extraction : Extraction meteo Open-Meteo -> Bronze -> Silver (Postgres)
- elt_e2e : Pipeline complet Extract -> Load -> dbt run -> dbt test (Postgres)
- elt_snowflake : Pipeline complet Extract -> PUT stage -> COPY INTO -> dbt run --target snowflake

### Deploiement des DAGs : GitDagBundle

Les DAGs ne sont pas copies manuellement dans les conteneurs : ils sont synchronises automatiquement depuis le depot GitHub via un GitDagBundle Airflow 3.x (airflow.providers.git.bundles.git.GitDagBundle), configure dans airflow/.env :

AIRFLOW__DAG_PROCESSOR__DAG_BUNDLE_CONFIG_LIST=[{"name":"my-git-bundle","classpath":"airflow.providers.git.bundles.git.GitDagBundle","kwargs":{"tracking_ref":"main","git_conn_id":"my_git_conn","subdir":"dags"}}]

Tout commit pousse sur la branche main (dossier dags/) est automatiquement repris par Airflow apres resynchronisation du dag-processor.

### CI/CD

Un workflow GitHub Actions (.github/workflows/ci.yml) valide chaque push sur dags/ :
- Lint des DAGs avec ruff
- Test de parsing (validite syntaxique de chaque fichier DAG)

### Pipeline elt_e2e (Postgres) -- detail

extract_meteo et extract_dvf s'executent en parallele, puis chargent respectivement bronze meteo et bronze dvf, puis run_dbt s'execute une fois les deux chargements termines, suivi de test_dbt.

### Pipeline elt_snowflake -- detail

Pour chaque source (meteo et dvf) : extraction, puis PUT dans le stage raw Snowflake, puis creation de la table bronze si besoin, puis COPY INTO bronze. Une fois les deux sources chargees, dbt run --target snowflake s'execute pour produire silver et gold.

---

## Transformation dbt

### Modeles

Couche Silver (Postgres) :
- meteo_quotidien : Meteo nettoyee + colonne calculee a_plu
- dvf_mutations : DVF nettoye, type, filtre (valeur fonciere > 0)

Couche Silver (Snowflake) :
- meteo_quotidien_snowflake : equivalent meteo, syntaxe Snowflake (TRY_TO_DATE, TRY_TO_DOUBLE)
- dvf_mutations_snowflake : equivalent DVF, syntaxe Snowflake (TRY_TO_NUMBER)

Couche Gold (Postgres) :
- dim_commune : Dimension commune (schema en etoile DVF)
- dim_type_local : Dimension type de bien (schema en etoile DVF)
- fact_mutations : Table de fait des mutations DVF
- mart_dvf_par_commune : Agregat DVF par commune (nb mutations, prix m2)
- mart_meteo_quotidien : Agregat meteo quotidien

Couche Gold (Snowflake) :
- mart_meteo_snowflake : Agregat meteo + indicateur is_rainy
- mart_dvf_par_commune_snowflake : Agregat DVF par commune (Snowflake)

### Sources

Declarees dans dbt/models/sources.yml : bronze.dvf_mutations, bronze.meteo_quotidien.

### Tests

11 tests dbt (unique, not_null) repartis sur les modeles silver et gold, declares dans dbt/models/schema.yml. Tous passent (PASS=11).

### Lancer dbt manuellement

cd dbt

Cible Postgres :
dbt run --exclude meteo_quotidien_snowflake mart_meteo_snowflake
dbt test --exclude meteo_quotidien_snowflake mart_meteo_snowflake

Cible Snowflake :
dbt run --target snowflake --select meteo_quotidien_snowflake mart_meteo_snowflake dvf_mutations_snowflake mart_dvf_par_commune_snowflake

### Documentation dbt

dbt docs generate --exclude meteo_quotidien_snowflake mart_meteo_snowflake
dbt docs serve --port 8082 --host 0.0.0.0

Puis ouvrir http://10.1.1.1:8082 (lineage graph, descriptions des modeles et colonnes).

---

## Snowflake et Infrastructure as Code

### Ressources deployees via Terraform (terraform/snowflake/)

- Warehouse PLATFORM_WH : Compute X-SMALL, auto-suspend 60s
- Database PLATFORM_DB : Base de donnees principale
- Schemas BRONZE, SILVER, GOLD : Medaillon
- Stage RAW_STAGE : Lac de donnees -- fichiers bruts
- Stage REFINED_STAGE : Lac de donnees -- zone raffinee (reservee)
- Role ROLE_ENGINEER : Tous droits sur Bronze/Silver/Gold
- Role ROLE_ANALYST : Lecture (SELECT) sur Gold uniquement

### Modele RBAC : utilisateur, role, privilege

- Utilisateur : identite qui se connecte a Snowflake (ex. svc_terraform). Un utilisateur n'a par defaut aucun droit.
- Role : conteneur de privileges, assigne a un ou plusieurs utilisateurs. Snowflake fonctionne uniquement par roles (jamais de droit direct a un utilisateur).
- Privilege : permission precise sur un objet (ex. USAGE sur un warehouse, SELECT sur une table, ALL PRIVILEGES sur un schema).

### Roles crees et justification

ROLE_ENGINEER : USAGE sur warehouse et database, ALL PRIVILEGES sur Bronze/Silver/Gold, READ/WRITE sur les stages.
Justification : represente l'equipe data engineering, doit pouvoir creer, charger et transformer les donnees dans toutes les couches du medaillon, ainsi que deposer/lire des fichiers dans le lac de donnees.

ROLE_ANALYST : USAGE sur warehouse, database et schema Gold, SELECT sur les tables Gold uniquement.
Justification : represente un analyste metier, n'a besoin que de consulter les donnees deja preparees (marts), sans acces aux donnees brutes (Bronze) ni a la logique de transformation (Silver). Principe du moindre privilege.

Cette separation illustre le principe de moindre privilege : chaque role recoit uniquement les droits necessaires a sa fonction, limitant la surface d'erreur et les risques de securite.

### Deploiement

cd terraform/snowflake
terraform init
terraform plan
terraform apply

20 ressources creees (Apply complete! Resources: 20 added, 0 changed, 0 destroyed.).

---

## Lac de donnees (Snowflake Stages)

### Localisation

Le lac de donnees est implemente via des stages internes Snowflake :
- PLATFORM_DB.BRONZE.RAW_STAGE : zone brute (fichiers sources tels que recus)
- PLATFORM_DB.BRONZE.REFINED_STAGE : zone raffinee (reservee, evolutions futures type Parquet)

### Convention de partitionnement

Les fichiers sont deposes dans RAW_STAGE avec une structure hierarchique par cle, facilitant les lectures ciblees :

@RAW_STAGE/dvf/annee=2025/dvf_2025.csv.gz
@RAW_STAGE/meteo/date=2026-06-27/meteo_2026-06-27.json

### Flux d'alimentation

1. Extraction : Airflow recupere les donnees (API Open-Meteo, fichier DVF data.gouv.fr) et les ecrit localement sur le worker.
2. PUT : une tache Airflow (snowflake.connector) depose le fichier brut dans RAW_STAGE via la commande SQL PUT.
3. COPY INTO : un operateur SQL Airflow (SQLExecuteQueryOperator) charge les fichiers du stage vers les tables Bronze (BRONZE.DVF_MUTATIONS, BRONZE.METEO_QUOTIDIEN).
4. dbt run --target snowflake : transforme Bronze -> Silver -> Gold avec la syntaxe SQL Snowflake.

### Verification

USE DATABASE PLATFORM_DB;
USE SCHEMA BRONZE;
LIST @RAW_STAGE;
SELECT COUNT(*) FROM BRONZE.DVF_MUTATIONS;
SELECT COUNT(*) FROM BRONZE.METEO_QUOTIDIEN;

---

## Gouvernance et gestion des secrets

Aucun mot de passe, token ou cle API n'est versionne en clair dans le code.

### Connexions et Variables Airflow (Admin -> Connections)

- postgres_warehouse (Postgres) : Acces a la base warehouse depuis les DAGs (Postgres)
- snowflake_platform (Snowflake) : Acces a Snowflake depuis les DAGs
- my_git_conn (Generic) : Authentification du GitDagBundle

Dans le code des DAGs, les credentials sont recuperes via :

from airflow.hooks.base import BaseHook
conn = BaseHook.get_connection("postgres_warehouse")
engine = create_engine(f"postgresql://{conn.login}:{conn.password}@{conn.host}:{conn.port}/{conn.schema}")

### Fichiers exclus du versioning (.gitignore)

- airflow/.env (reel, non versionne) -- voir .env.example
- dbt/profiles.yml (reel, non versionne) -- voir dbt/profiles.yml.example
- terraform/snowflake/terraform.tfvars (reel, non versionne) -- voir terraform.tfvars.example
- *.tfstate*, .terraform/ (non versionnes)

---

## Gestion des erreurs

- Retries : les taches d'extraction (extract_meteo, extract_dvf) sont configurees avec retries=3 et retry_delay exponentiel (retry_exponential_backoff=True), pour absorber les erreurs reseau transitoires.
- Notification d'echec : un callback on_failure_callback envoie une notification vers ntfy.sh (topic airflow-platform-data-notifications) en cas d'echec d'une tache, avec le dag_id, task_id, run_id et l'horodatage.
- ON_ERROR = 'CONTINUE' sur les COPY INTO Snowflake : permet de ne pas bloquer un chargement bronze sur quelques lignes malformees.

---

## Structure du projet

platform-data/
- README.md (ce fichier)
- docker-compose.yml (infra Postgres/Redis/pgAdmin/code-server)
- .gitignore
- dags/ (DAGs Airflow, synchronises via GitDagBundle)
  - hello_dag.py
  - dvf_extraction.py
  - openmeteo_extraction.py
  - elt_e2e.py
  - elt_snowflake.py
- airflow/
  - docker-compose.yaml (compose Airflow, CeleryExecutor)
  - .env.example (modele de configuration, sans secrets)
- dbt/ (projet dbt)
  - dbt_project.yml
  - profiles.yml.example (modele de configuration, sans secrets)
  - models/
    - sources.yml
    - schema.yml (tests et descriptions)
    - silver/
    - gold/
- terraform/
  - snowflake/
    - versions.tf
    - variables.tf
    - main.tf
    - terraform.tfvars.example
- .github/
  - workflows/
    - ci.yml (CI : lint + parsing des DAGs)

---

## Auteur

Christian MANSOUR -- UE Plateforme de donnees sur le cloud
