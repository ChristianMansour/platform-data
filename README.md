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
