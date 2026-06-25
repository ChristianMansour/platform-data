# Platform Data - Infrastructure

Ce projet contient l'infrastructure de la plateforme data avec PostgreSQL, Redis, pgAdmin et VS Code Server.

## Prérequis

- Docker et Docker Compose installés
- Accès VPN WireGuard actif
- Accès SSH à la VM

## Structure des données

/opt/
├── docker/                    # Docker data-root (images, conteneurs)
├── docker-volumes/
│   ├── postgres/             # Données PostgreSQL
│   ├── redis/                # Données Redis
│   ├── pgadmin/              # Configuration pgAdmin
│   └── code-server/          # Configuration VS Code Server

## Installation et lancement

### 1. Créer le réseau Docker

    docker network create platform-net

### 2. Créer les dossiers pour les volumes

    mkdir -p /opt/docker-volumes/postgres
    mkdir -p /opt/docker-volumes/redis
    mkdir -p /opt/docker-volumes/pgadmin
    mkdir -p /opt/docker-volumes/code-server

### 2.5 Configurer les permissions pgAdmin

    sudo chown -R 5050:5050 /opt/docker-volumes/pgadmin

### 3. Lancer les services

Depuis le répertoire du projet (~/platform-data) :

    docker compose up -d

### 4. Vérifier que les services tournent

    docker ps

Vous devez voir 4 conteneurs : postgres-warehouse, redis, pgadmin, code-server.

## Accès aux services

### PostgreSQL

- Host : 10.1.1.1 (depuis l'extérieur) ou postgres (depuis Docker)
- Port : 5432
- Database : warehouse
- User : platform
- Password : platform

### Redis

- Host : 10.1.1.1 (depuis l'extérieur) ou redis (depuis Docker)
- Port : 6379

### pgAdmin

- URL : http://10.1.1.1:5050
- Email : admin@admin.com
- Password : admin

Note : les permissions du volume pgAdmin doivent appartenir à l'UID 5050.

### VS Code Server

- URL : http://10.1.1.1:8080
- Password : admin

## Vérification de l'installation

### Test PostgreSQL

    docker exec -it postgres-warehouse psql -U platform -d warehouse -c "SELECT 1 AS test;"

### Test Redis

    docker exec -it redis redis-cli PING

## Arrêt des services

    docker compose down

## Flux ELT (Extraction → Chargement → Transformation)

### Architecture du pipeline

Le pipeline ELT complet est orchestré par Airflow et suit l'approche médaillon (Bronze → Silver → Gold).

**Sources de données :**
- DVF 2025 (data.gouv.fr) : fichier CSV des mutations immobilières
- Open-Meteo (API) : données météo quotidiennes pour Marseille

**DAGs Airflow disponibles :**

| DAG | Rôle |
|---|---|
| `dvf_2025_extraction` | Extraction DVF → Bronze → Silver |
| `openmeteo_extraction` | Extraction météo → Bronze → Silver |
| `elt_e2e` | Pipeline complet E→L→T (extraction + chargement + dbt run + dbt test) |
| `elt_snowflake` | Pipeline E→L→T sur Snowflake |

### Lancer le flux ELT complet

1. Ouvrir l'UI Airflow : http://10.1.1.1:8081 (login `airflow`/`airflow`)
2. Activer et déclencher le DAG `elt_e2e`
3. Le DAG enchaîne automatiquement :
   - Extraction météo + DVF (parallèle)
   - Chargement Bronze (météo + DVF)
   - `dbt run` (construction Silver + Gold)
   - `dbt test` (validation qualité des données)

### Où voir les données

- **Bronze** : schéma `bronze` (données brutes)
- **Silver** : schéma `bronze_silver` (données nettoyées et typées)
- **Gold** : schéma `bronze_gold` (marts métier, schéma en étoile DVF)

Consultable via pgAdmin (http://10.1.1.1:5050) ou directement en SQL :
```sql
SELECT * FROM bronze_gold.fact_mutations LIMIT 10;
SELECT * FROM bronze_gold.mart_meteo_quotidien;
```

### Documentation dbt

```bash
cd ~/platform-data/dbt
dbt docs generate --exclude meteo_quotidien_snowflake mart_meteo_snowflake
dbt docs serve --port 8082 --host 0.0.0.0
```
Puis ouvrir http://10.1.1.1:8082

## Gouvernance et gestion des secrets

Aucun mot de passe, token ou clé API n'est versionné en clair dans le code.

### Connexions Airflow (Admin → Connections)

| Connection ID | Type | Usage |
|---|---|---|
| `postgres_warehouse` | Postgres | Accès à la base `warehouse` depuis les DAGs |
| `my_git_conn` | Generic | Authentification GitDagBundle |

Dans le code des DAGs, les credentials sont récupérés via :
```python
from airflow.hooks.base import BaseHook
conn = BaseHook.get_connection("postgres_warehouse")
```

### Fichiers exclus du versioning (`.gitignore`)

- `airflow/.env` (clé Fernet, identifiants Airflow)
- `dbt/profiles.yml` (credentials Postgres + Snowflake) — voir `dbt/profiles.yml.example` pour la structure attendue
- `terraform.tfvars`, `*.tfstate*`
