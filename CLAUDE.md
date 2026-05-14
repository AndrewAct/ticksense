# TickSense

Real-time crypto market lakehouse: Binance L2 WebSocket → Kafka → Flink → Iceberg → Trino → FastAPI.
Target: ~5M ticks/day, 50 pairs, <30s end-to-end freshness.

## Stack

| Layer | Technology |
|---|---|
| Ingestion | Python 3.12, websockets, aiokafka |
| Broker | Redpanda (Kafka-compatible) |
| Processing | PyFlink |
| Storage | Apache Iceberg on MinIO (local) / GCS (cloud) |
| Query | Trino |
| Analytics | dbt |
| Orchestration | Airflow |
| API | FastAPI + Pydantic v2 |
| CDC | Postgres + Debezium |
| Packaging | uv workspace |

## Non-negotiable rules

- Every feature includes tests. Coverage must not decrease.
- Python must pass ruff, mypy, and pytest before any commit.
- No secrets in repo. All config via environment variables + pydantic-settings.
- All Kafka messages have deterministic keys (see conventions below).
- All streaming jobs define: event time field, watermark strategy, dedup key, DLQ behavior.
- All Airflow and Spark jobs are idempotent and safe to rerun with the same parameters.
- All FastAPI endpoints have Pydantic request and response models.
- All Iceberg tables preserve `kafka_topic`, `kafka_partition`, `kafka_offset` for replay.
- Prefer explicit schemas over untyped dicts at every system boundary.

## Kafka conventions

Topic naming:
- `market.raw.orderbook`       — raw L2 depth snapshots + diffs
- `market.normalized.trades`   — deduplicated, typed events
- `market.agg.ohlcv_1m`        — 1-minute OHLCV aggregates
- `market.signal.events`       — derived liquidity / spread signals
- `postgres.public.<table>`    — CDC events from Debezium
- `market.dlq`                 — dead letter queue

Message key:
- Market events: `{exchange}#{symbol}`
- CDC events: `{table}#{primary_key}`

Dedup key:
- L2 events: `{exchange}#{symbol}#{last_update_id}`
- Trades: `{exchange}#{symbol}#{trade_id}`

## Iceberg table layers

- **bronze** (`raw.*`): append-only, raw payload + full Kafka metadata preserved
- **silver** (`normalized.*`): typed, deduplicated, event-time indexed
- **gold** (`marts.*`): aggregated, business-facing, safe for Trino / API

## Development workflow

```bash
make up          # start local stack (Redpanda, Postgres, MinIO, Iceberg, Trino, Flink)
make down        # stop and remove volumes
make lint        # ruff check + format check
make format      # ruff format + autofix
make typecheck   # mypy across all workspace members
make test        # pytest across all workspace members
make coverage    # pytest --cov, fails below 80%
```

## Before marking work complete

Run `make lint typecheck test coverage` and confirm all pass.

If touching Docker config: `docker compose config` must succeed.
If touching dbt: `dbt compile && dbt test` must pass.

## Skills

Specialized constraints and patterns live in `.skills/`. Read the relevant skill before implementing each layer:

- Kafka code → `.skills/kafka-streaming-patterns/SKILL.md`
- Flink jobs → `.skills/flink-stream-processing/SKILL.md`
- Iceberg tables → `.skills/lakehouse-iceberg-patterns/SKILL.md`
- CDC / Debezium → `.skills/cdc-debezium-patterns/SKILL.md`
- Python services → `.skills/production-python-service/SKILL.md`
- Tests → `.skills/testing-and-coverage/SKILL.md`
- Airflow / Spark → `.skills/spark-airflow-backfill/SKILL.md`
- dbt models → `.skills/dbt-analytics-engineering/SKILL.md`
- CI / quality gates → `.skills/ci-cd-quality-gates/SKILL.md`
- Local dev stack → `.skills/docker-compose-local-dev/SKILL.md`
