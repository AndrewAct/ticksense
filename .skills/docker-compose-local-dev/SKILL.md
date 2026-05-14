---
name: docker-compose-local-dev
description: Use when modifying docker-compose.yml, adding services to the local stack, or setting up local development for the first time.
---

# Docker Compose Local Dev

## Services in the local stack

```
redpanda            Kafka-compatible broker (lighter than Kafka, no ZooKeeper)
redpanda-console    Web UI for topic inspection (localhost:8080)
postgres            Source OLTP (Debezium) + Airflow metadata backend
debezium            Kafka Connect with Debezium Postgres connector
minio               S3-compatible object store for Iceberg data
iceberg-rest        Iceberg REST catalog (backed by MinIO)
trino               Query engine over Iceberg tables
flink-jobmanager    PyFlink job manager
flink-taskmanager   PyFlink task manager (1 replica for local)
```

## Port assignments (local)

```
9092   Redpanda Kafka (broker)
19092  Redpanda Kafka (external / host access)
8080   Redpanda Console
9000   MinIO API
9001   MinIO Console
8181   Iceberg REST catalog
8082   Trino
5432   Postgres
8083   Debezium / Kafka Connect REST API
8081   Flink JobManager UI
```

## Requirements for every service

- `healthcheck` defined with realistic `start_period`
- Named volumes for stateful data (not bind mounts)
- `env_file: .env` for secrets; no credentials in `docker-compose.yml`
- `restart: unless-stopped` for stateful services
- `depends_on` with `condition: service_healthy` for ordering

## Startup

```bash
make up    # docker compose up -d --wait
make down  # docker compose down -v  (removes volumes — clean slate)
```

`--wait` blocks until all healthchecks pass, so `make up` succeeding means the stack is ready.

## Verifying the stack

After `make up`:
```bash
# Redpanda
rpk cluster info --brokers localhost:19092

# MinIO
mc alias set local http://localhost:9000 minioadmin minioadmin

# Trino
docker compose exec trino trino --execute "SHOW CATALOGS"

# Postgres
psql postgresql://ticksense:changeme@localhost:5432/ticksense -c "SELECT 1"
```

## Anti-patterns

- Bind mounts for database data (`./data:/var/lib/postgresql`) — breaks on Mac/Linux file permission differences
- No healthchecks — services appear ready but dependencies crash on startup
- Hard-coded passwords in `docker-compose.yml`
- Services that cannot restart cleanly without `docker compose down -v`
- Sharing port 8080 between Redpanda Console and Trino (use 8082 for Trino)
