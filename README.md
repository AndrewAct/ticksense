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

## Getting started

```bash
cp .env.example .env
make up       # start the full local stack
make test     # run all tests
make lint     # ruff + format check
```

## Development

```bash
make typecheck     # mypy strict
make coverage      # pytest --cov, fails below 80%
make dbt-run       # run dbt models
make replay-sample # replay 10k events (dry-run)
make down          # tear down + remove volumes
```

## Project layout

```
ingest/    Binance WebSocket ingestion → Kafka
api/       FastAPI query layer over Trino
replay/    Kafka offset replay producer
flink/     PyFlink streaming jobs
airflow/   Orchestration DAGs
dbt/       Analytics models (staging → intermediate → marts)
spark/     Backfill and compaction jobs
infra/     Docker configs for local stack
.skills/   Engineering playbooks for each layer
```

## Tech stack

Python 3.12 · uv · Redpanda · PyFlink · Apache Iceberg · MinIO · Trino · dbt · Airflow · FastAPI · Pydantic v2 · Postgres · Debezium
