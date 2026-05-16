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

## Getting started

### Prerequisites

| Tool | Version | Purpose |
|---|---|---|
| [Docker Desktop](https://www.docker.com/products/docker-desktop/) | ≥ 4.x | All infrastructure services |
| [uv](https://docs.astral.sh/uv/getting-started/installation/) | ≥ 0.5 | Python package manager for the main workspace |
| Python ≥3.12 | 3.13 recommended (Homebrew) | Main workspace (ingest, api, replay) |
| Python 3.10 | exactly 3.10 | Flink IDE venv only (must be on PATH as `python3.10`) |

Install uv:
```bash
curl -LsSf https://astral.sh/uv/install.sh | sh
```

Install Python versions via [pyenv](https://github.com/pyenv/pyenv) or [python.org](https://www.python.org/downloads/):
```bash
pyenv install 3.13 3.10   # if using pyenv
```

---

### Step 1 — create the main Python environment

```bash
uv sync --all-packages   # creates .venv with Python 3.12 and installs all workspace members
```

This installs `ingest`, `api`, `replay` and all their dependencies. The `--all-packages` flag is required — plain `uv sync` only resolves the root package and leaves workspace members uninstalled. All `uv run` / `make lint|test|typecheck` commands use this venv automatically.

---

### Step 2 — configure environment variables

```bash
cp .env.example .env
```

The defaults in `.env.example` work out-of-the-box for local development (MinIO, Redpanda, Postgres all use the Docker-compose credentials). No API keys are required — Binance public WebSocket data is used.

---

### Step 3 — start the infrastructure stack

```bash
make up
```

Pulls and starts: Redpanda, MinIO, Iceberg REST, Trino, Postgres, Flink (jobmanager + taskmanager). Also submits both Flink jobs automatically via `flink-init`.

First boot pulls ~3 GB of images and takes a few minutes. Subsequent `make up` takes ~30s.

Once healthy, services are available at:

| Service | URL |
|---|---|
| Redpanda Console | http://localhost:8080 |
| MinIO Console | http://localhost:9001 (minioadmin / minioadmin) |
| Flink UI | http://localhost:8081 |
| Trino UI | http://localhost:8082 |
| Iceberg REST | http://localhost:8181/v1/config |

---

### Step 4 — create the Flink IDE venv (optional, for IDE navigation)

`flink/` is not part of the uv workspace because `apache-flink` cannot be built from
source with current pip/setuptools versions. Without this step, your IDE cannot resolve
`pyflink` imports. The venv copies pyflink directly from the running container:

```bash
make flink-venv   # requires: make up has already been run
```

Then point your IDE at `flink/.venv/bin/python`:

- **VS Code**: `Cmd+Shift+P` → `Python: Select Interpreter` → `./flink/.venv/bin/python`
- **PyCharm**: `Settings → Project → Python Interpreter → Add → Existing → flink/.venv/bin/python`

Re-run `make flink-venv` after `make build` if the Flink image is rebuilt.

---

### Step 5 — start ingestion

The ingest service runs outside Docker (pure Python, no container needed):

```bash
uv run python -m ingest.main
```

This opens a WebSocket to Binance and streams L2 order book events into Redpanda. Stop with `Ctrl+C` — the producer flushes before exit.

Override symbols for a quick test:
```bash
INGEST_SYMBOLS=btcusdt uv run python -m ingest.main
```

---

### Verify everything is flowing

```bash
# Flink: both jobs should show RUNNING
curl -s http://localhost:8081/jobs/overview | python3 -m json.tool

# Redpanda: messages arriving
docker compose exec redpanda rpk topic consume market.raw.orderbook \
  --brokers localhost:9092 --num 5

# Run all tests
make test
```

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
make watch-cdc      # tail postgres.public.symbol_config topic live
make build          # docker compose build --no-cache
```

### Running only unit tests (no Docker needed)

```bash
uv run pytest ingest/tests/unit -v
uv run pytest --ignore=ingest/tests/integration -q
```

### Flink IDE setup

`flink/` is **not** part of the uv workspace because `apache-flink` cannot be built
from source with current pip/setuptools versions. Without a local pyflink install, your
IDE cannot resolve `pyflink` imports — go-to-definition and type hints will be broken.

The fix is a separate Python 3.10 venv (matching the container's runtime) that gets
pyflink copied directly from the running `flink-jobmanager` container, bypassing the
broken pip build entirely:

```bash
make up          # stack must be running first
make flink-venv  # creates flink/.venv with pyflink + py4j + cloudpickle
```

Then point your IDE at `flink/.venv/bin/python`:

- **VS Code**: `Cmd+Shift+P` → `Python: Select Interpreter` → choose
  `./flink/.venv/bin/python`
- **PyCharm**: `Settings → Project → Python Interpreter → Add → Existing
  → flink/.venv/bin/python`

`flink/.venv` is covered by the existing `.venv/` rule in `.gitignore` — it is never committed.

> Re-run `make flink-venv` after `make build` if the Flink image is rebuilt.

---

### Before commit 

* Run `make lint typecheck test coverage` to ensure tests have passed

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
| Ingestion | Python 3.13, websockets, aiokafka, httpx |
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
| 2 — Flink Processing | ✅ | Normalize, dedup, OHLCV windows → Iceberg silver (e2e verified 2026-05-15) |
| 3 — CDC + Replay | ✅ | Debezium CDC, Flink upsert job, replay CLI (e2e verified 2026-05-16) |
| 4 — Analytics Layer | — | dbt models, FastAPI |
| 5 — Ops + Observability | — | Airflow, Great Expectations, SLA alerts |
| 6 — Demo + Blog + Web | — | ticksense.ai, demo video, blog posts |
