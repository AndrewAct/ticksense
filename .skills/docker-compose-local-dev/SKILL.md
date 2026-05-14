---
name: docker-compose-local-dev
description: Use when modifying docker-compose.yml, adding services to the local stack, or setting up local development for the first time.
---

# Docker Compose Local Dev

## Services in the local stack

```
redpanda            Kafka-compatible broker (lighter than Kafka, no ZooKeeper)
redpanda-console    Web UI for topic inspection (localhost:8080)
redpanda-init       One-shot: creates market.raw.orderbook + market.dlq topics
postgres            Source OLTP (Debezium) + Airflow metadata backend
minio               S3-compatible object store for Iceberg data
minio-init          One-shot: creates the ticksense bucket
iceberg-rest        Iceberg REST catalog (backed by MinIO)
trino               Query engine over Iceberg tables
flink-jobmanager    PyFlink job manager
flink-taskmanager   PyFlink task manager (1 replica for local)
debezium            Kafka Connect with Debezium Postgres connector (Phase 3)
```

## Port assignments (local)

```
19092  Redpanda Kafka (external / host access)
8080   Redpanda Console
9000   MinIO API
9001   MinIO Console  (login: minioadmin / minioadmin)
8181   Iceberg REST catalog
8082   Trino (mapped from internal 8080)
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

## Startup and shutdown

```bash
make up    # docker compose up -d --wait  — blocks until all healthchecks pass
make down  # docker compose down -v       — stops AND removes all volumes (clean slate)

docker compose stop  # stop containers but KEEP volumes (data survives)
docker compose up -d --wait  # restart after stop, volumes intact
```

`--wait` blocks until all healthchecks pass, so `make up` succeeding means the stack is ready.

## One-shot init containers

Use `restart: "no"` for init containers (topic creation, bucket creation):

```yaml
redpanda-init:
  restart: "no"
  depends_on:
    redpanda:
      condition: service_healthy
```

`make down` removes them; they re-run on next `make up`. Trino's `depends_on` should only reference
long-lived services (not init containers), so it doesn't get blocked waiting for them.

## Healthcheck patterns by image

Different images have different tools available — `curl` is not universal:

| Image | Tool | Example |
|---|---|---|
| redpanda | `rpk` (built-in) | `rpk cluster health \| grep -qE 'Healthy:.+true'` |
| postgres | `pg_isready` (built-in) | `pg_isready -U ticksense -d ticksense` |
| minio | `curl` (available) | `curl -sf http://localhost:9000/minio/health/live` |
| trino | `curl` (available) | `curl -sf http://localhost:8080/v1/info \| grep -q '"starting":false'` |
| tabulario/iceberg-rest | **no curl/wget** | `bash -c 'echo > /dev/tcp/localhost/8181'` |
| flink | `curl` (available) | `curl -sf http://localhost:8081/overview` |

**`tabulario/iceberg-rest` does NOT have `curl` or `wget`.**
Use bash TCP redirect: `bash -c 'echo > /dev/tcp/localhost/8181'`

## Verifying the stack

After `make up`:

```bash
# Redpanda
docker compose exec redpanda rpk cluster health --brokers localhost:9092
docker compose exec redpanda rpk topic list --brokers localhost:9092

# MinIO
curl -sf http://localhost:9000/minio/health/live && echo "OK"

# Iceberg REST
curl -sf http://localhost:8181/v1/config

# Trino
docker compose exec trino trino --execute "SHOW CATALOGS"

# Postgres
docker compose exec postgres pg_isready -U ticksense -d ticksense
```

## Flink + S3 (MinIO) configuration

Flink uses `FLINK_PROPERTIES` env var as a multi-line string. S3 config goes in the same block:

```yaml
environment:
  FLINK_PROPERTIES: |
    jobmanager.rpc.address: flink-jobmanager
    taskmanager.numberOfTaskSlots: 4
    state.backend: filesystem
    state.checkpoints.dir: s3://ticksense/flink-checkpoints
    s3.access-key: minioadmin
    s3.secret-key: minioadmin
    s3.endpoint: http://minio:9000
    s3.path.style.access: true
```

## Trino Iceberg catalog config

Trino reads catalog properties from a mounted directory. Structure:

```
infra/config/trino/
  config.properties       coordinator, http port, memory limits
  node.properties         node.environment, node.id
  jvm.config              JVM heap flags
  log.properties          io.trino=INFO
  catalog/
    iceberg.properties    connector, REST catalog URI, S3 config
```

Key iceberg catalog settings:

```properties
connector.name=iceberg
iceberg.catalog.type=rest
iceberg.rest-catalog.uri=http://iceberg-rest:8181
fs.native-s3.enabled=true
s3.endpoint=http://minio:9000
s3.path-style-access=true
```

## Iceberg REST + MinIO dependency chain

```
minio (healthy)
  └── minio-init (completed_successfully)
        └── iceberg-rest (healthy)
              └── trino (healthy)
```

`iceberg-rest` must wait for `minio-init` to complete (bucket must exist before catalog starts).
Use `condition: service_completed_successfully` for init containers in `depends_on`.

## Anti-patterns

- Bind mounts for database data (`./data:/var/lib/postgresql`) — breaks on Mac/Linux file permission differences
- No healthchecks — services appear ready but dependencies crash on startup
- Hard-coded passwords in `docker-compose.yml`
- Services that cannot restart cleanly without `docker compose down -v`
- Sharing port 8080 between Redpanda Console and Trino (use 8082 for Trino's external port)
- Using `curl` in healthcheck for images that don't have it (iceberg-rest, some Java images)
- Forgetting `restart: "no"` on init containers — they'll loop forever trying to re-run
