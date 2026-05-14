# TickSense

Real-time crypto market lakehouse built end-to-end.

**Binance L2 WebSocket → Kafka → Flink → Iceberg on MinIO → dbt over Trino → FastAPI**

~5M ticks/day · 50 pairs · <30s end-to-end freshness · Postgres CDC via Debezium

## Architecture

```
Binance WebSocket (L2 order book)
        │
        ▼
   Kafka (Redpanda)  ◄─── Debezium CDC ◄─── Postgres OLTP
        │
        ▼
   Flink (PyFlink)
   ├── normalize + deduplicate
   ├── order book state machine → spread, imbalance, microprice
   └── tumbling window → OHLCV 1m
        │
        ▼
   Iceberg on MinIO/GCS
   ├── raw.*        (append-only, full Kafka metadata)
   ├── normalized.* (typed, deduped)
   └── marts.*      (aggregated, API-facing)
        │
        ▼
   dbt over Trino → FastAPI
```

---

## Quick start

```bash
cp .env.example .env
make up        # pull images, start all services, wait for healthchecks
make test      # run all unit + integration tests
```

First boot pulls ~3 GB of images; subsequent `make up` takes ~30s.

---

## Local stack — service UIs

Once `make up` succeeds, every service is reachable from your browser or CLI:

| Service | URL | What you'll see |
|---|---|---|
| Redpanda Console | http://localhost:8080 | Topics, consumer groups, message browser |
| MinIO Console | http://localhost:9001 | Object store — `ticksense/warehouse/` holds Iceberg data files |
| Flink UI | http://localhost:8081 | Running jobs, task managers, checkpoints, exceptions |
| Trino UI | http://localhost:8082 | Query history, cluster overview |
| Iceberg REST | http://localhost:8181/v1/config | Catalog config (JSON) |

MinIO login: `minioadmin` / `minioadmin`

---

## Starting ingestion

The `ingest` service streams Binance L2 order book diffs over WebSocket and produces to Redpanda. It runs outside Docker (pure Python).

```bash
# Start ingesting (default symbols from .env: btcusdt,ethusdt,solusdt,bnbusdt,xrpusdt)
uv run python -m ingest.main

# Override symbols for a quick test
INGEST_SYMBOLS=btcusdt uv run python -m ingest.main
```

Stop with `Ctrl+C` — the producer flushes before exit.

---

## Verifying data is flowing

### Redpanda — messages in topic

```bash
# Count messages in the raw orderbook topic
docker compose exec redpanda rpk topic consume market.raw.orderbook \
  --brokers localhost:9092 --num 5 --format json

# Lag per consumer group
docker compose exec redpanda rpk group list --brokers localhost:9092

# Topic metadata
docker compose exec redpanda rpk topic describe market.raw.orderbook --brokers localhost:9092
```

### Redpanda Console (browser)

1. Open http://localhost:8080
2. Click **Topics → market.raw.orderbook**
3. Click **Messages** — live stream of order book events

### Trino — query Iceberg tables (Phase 2+)

```bash
# Interactive Trino CLI
docker compose exec trino trino --catalog iceberg

# From inside the CLI:
SHOW SCHEMAS FROM iceberg;
SHOW TABLES FROM iceberg.raw;
SELECT count(*) FROM iceberg.raw.orderbook_diffs;
SELECT symbol, count(*) FROM iceberg.raw.orderbook_diffs GROUP BY 1 ORDER BY 2 DESC;
```

Or open the Trino web UI at http://localhost:8082 and run queries there.

### MinIO — check Iceberg data files

1. Open http://localhost:9001 (login: minioadmin / minioadmin)
2. Browse `ticksense → warehouse → raw → orderbook_diffs`
3. Parquet files appear after Flink writes the first checkpoint

---

## Stopping services

```bash
make down           # stop all containers AND delete volumes (clean slate)
docker compose stop # stop containers but KEEP volumes (data survives restart)
docker compose up -d --wait  # restart after `docker compose stop`
```

Use `make down` when you want a fresh environment. Use `docker compose stop` + `up` when you want to pause and resume without losing data.

---

## Stack management

```bash
make up             # start full stack, block until healthy
make down           # stop + remove all volumes
make restart        # docker compose restart (rolling restart, keeps volumes)
make logs           # tail all service logs
docker compose logs -f redpanda   # tail a single service
docker compose ps                 # show running containers + ports
```

### Checking individual service health

```bash
# Redpanda cluster
docker compose exec redpanda rpk cluster health --brokers localhost:9092

# Postgres
docker compose exec postgres pg_isready -U ticksense -d ticksense

# MinIO
curl -sf http://localhost:9000/minio/health/live && echo "OK"

# Iceberg REST
curl -sf http://localhost:8181/v1/config

# Trino
curl -sf http://localhost:8082/v1/info | python3 -m json.tool
```

---

## Development commands

```bash
make lint           # ruff check + format check
make format         # ruff format + autofix
make typecheck      # mypy strict across all workspace members
make test           # pytest (unit + integration)
make coverage       # pytest --cov, fails below 80%
make dbt-compile    # compile dbt models
make dbt-run        # run dbt models
make dbt-test       # run dbt tests
make replay-sample  # replay 10k events dry-run
make build          # docker compose build --no-cache
```

### Running only unit tests (no Docker needed)

```bash
uv run pytest ingest/tests/unit -v
uv run pytest --ignore=ingest/tests/integration -q
```

---

## Project layout

```
ingest/    Binance WebSocket ingestion → Kafka
  src/ingest/
    settings.py     pydantic-settings config
    models.py       BinanceDepthEvent, OrderBookEvent, DLQEvent
    orderbook.py    Order book state machine (snapshot + diff)
    producer.py     aiokafka producer, idempotent, DLQ routing
    client.py       Async WS client, reconnect, backoff
    main.py         Entry point: asyncio.gather over symbols

api/       FastAPI query layer over Trino
replay/    Kafka offset replay producer
flink/     PyFlink streaming jobs (Phase 2)
airflow/   Orchestration DAGs (Phase 5)
dbt/       Analytics models: staging → intermediate → marts (Phase 4)
spark/     Backfill and compaction jobs (Phase 5)
infra/
  config/
    redpanda/     console.yaml
    trino/        config.properties, catalog/iceberg.properties
    debezium/     connector configs (Phase 3)
.skills/   Engineering playbooks for each layer
docs/      ROADMAP.md
```

---

## Tech stack

| Layer | Technology |
|---|---|
| Ingestion | Python 3.12, websockets, aiokafka, httpx |
| Broker | Redpanda v24.3 (Kafka-compatible, no ZooKeeper) |
| Processing | PyFlink 1.18 |
| Storage | Apache Iceberg 1.6 on MinIO (local) / GCS (cloud) |
| Catalog | Iceberg REST (tabulario) |
| Query | Trino 457 |
| Analytics | dbt |
| Orchestration | Airflow |
| API | FastAPI + Pydantic v2 |
| CDC | Postgres 16 + Debezium |
| Packaging | uv workspace |
| Testing | pytest, testcontainers, pytest-asyncio |

---

## Roadmap

See [docs/ROADMAP.md](docs/ROADMAP.md) for the full phase plan.

| Phase | Status | Description |
|---|---|---|
| 0 — Scaffolding | ✅ | CLAUDE.md, skills, uv workspace, CI, Makefile |
| 1 — Local Stack + Ingest | ✅ | docker-compose, Binance WS → Kafka (57 tests, 96% cov) |
| 2 — Flink Processing | ⏳ | Normalize, dedup, OHLCV windows → Iceberg silver |
| 3 — CDC + Replay | — | Debezium, replay producer |
| 4 — Analytics Layer | — | dbt models, FastAPI |
| 5 — Ops + Observability | — | Airflow, Great Expectations, SLA alerts |
| 6 — Demo + Blog + Web | — | ticksense.ai, demo video, blog posts |
