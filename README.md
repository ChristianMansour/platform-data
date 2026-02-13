# Platform Data - Infrastructure

Ce projet contient l'infrastructure de la plateforme data avec PostgreSQL, Redis et pgAdmin.

## Prérequis

- Docker et Docker Compose installés
- Accès VPN WireGuard actif
- Accès SSH à la VM

## Structure des données
```
/opt/
├── docker/                    # Docker data-root (images, conteneurs)
├── docker-volumes/
│   ├── postgres/             # Données PostgreSQL
│   ├── redis/                # Données Redis
│   └── pgadmin/              # Configuration pgAdmin
```

## Installation et lancement

### 1. Créer le réseau Docker
```bash
docker network create platform-net
```

### 2. Lancer les services

Depuis le répertoire du projet (`~/platform-data`) :
```bash
docker compose up -d
```

### 3. Vérifier que les services tournent
```bash
docker ps
```

Vous devez voir 3 conteneurs : `postgres-warehouse`, `redis`, `pgadmin`.

## Accès aux services

### PostgreSQL
- **Host** : `10.1.1.1` (depuis l'extérieur) ou `postgres` (depuis Docker)
- **Port** : `5432`
- **Database** : `warehouse`
- **User** : `platform`
- **Password** : `platform`

### Redis
- **Host** : `10.1.1.1` (depuis l'extérieur) ou `redis` (depuis Docker)
- **Port** : `6379`

### pgAdmin
- **URL** : http://10.1.1.1:5050
- **Email** : admin@admin.com
- **Password** : admin

## Vérification de l'installation

### Test PostgreSQL
```bash
docker exec -it postgres-warehouse psql -U platform -d warehouse -c "SELECT 1 AS test;"
```

### Test Redis
```bash
docker exec -it redis redis-cli PING
```

## Arrêt des services
```bash
docker compose down
```


