# TickSense Roadmap

Demo-first project: build a production-style crypto market lakehouse locally,
record a demo, publish a blog and landing page.

---

## Phase 0 — Scaffolding ✅

**Goal:** Establish engineering standards before writing any application code.

- [x] `CLAUDE.md` — project constitution
- [x] `.skills/` — 10 engineering playbooks
- [x] uv workspace (`ingest/`, `api/`, `replay/`)
- [x] `Makefile` — `make up/down/lint/test/coverage`
- [x] `.github/workflows/ci.yml` — lint + typecheck + test
- [x] Directory structure (`flink/`, `airflow/`, `dbt/`, `spark/`, `infra/`)
- [x] `.env.example`, `.gitignore`, `README.md`

**Done when:** `git log` shows a clean Phase 0 commit.

---

## Phase 1 — Local Stack + Ingestion ✅

**Goal:** Binance L2 order book data flowing from WebSocket into raw Iceberg tables.

### 1a — docker-compose.yml ✅

| Service | Image | Purpose |
|---|---|---|
| redpanda | redpandadata/redpanda:v24.3.1 | Kafka broker |
| redpanda-console | redpandadata/console:v2.7.2 | Topic browser (localhost:8080) |
| redpanda-init | redpandadata/redpanda | Creates `market.raw.orderbook` + `market.dlq` topics |
| postgres | postgres:16 | OLTP + Airflow backend (WAL logical replication enabled) |
| minio | minio/minio | S3-compatible object store (localhost:9000/9001) |
| minio-init | minio/mc | Creates `ticksense` bucket |
| iceberg-rest | tabulario/iceberg-rest:1.6.0 | Iceberg REST catalog (localhost:8181) |
| trino | trinodb/trino:457 | Query engine (localhost:8082), Iceberg catalog configured |
| flink-jobmanager | flink:1.18-scala_2.12-java11 | PyFlink job manager (localhost:8081) |
| flink-taskmanager | flink:1.18-scala_2.12-java11 | PyFlink task manager |
| debezium | debezium/connect | CDC (added in Phase 3) |

`make up` starts all services and blocks until healthchecks pass.

### 1b — ingest package ✅

```
ingest/src/ingest/
  settings.py        Settings via pydantic-settings
  models.py          Pydantic models: BinanceDepthEvent, BinanceDepthSnapshot,
                     OrderBookEvent, DLQEvent
  orderbook.py       Order book state machine (snapshot + diff maintenance)
  producer.py        aiokafka producer, idempotent, DLQ routing
  client.py          Async WebSocket client, snapshot buffering, reconnect logic
  main.py            Entry point: asyncio.gather over all symbols
```

**Key behaviors (all implemented):**
- On startup: REST snapshot fetched concurrently → buffer WebSocket diffs → apply in order
- On sequence gap: raise `GapDetectedError` → caller reconnects with exponential backoff
- On serialization error: route to `market.dlq`, log, continue
- Graceful shutdown: `contextlib.suppress(KeyboardInterrupt)` + `CancelledError` handler flushes producer cleanly

**Deliverables:**
- [x] `docker-compose.yml` with all services + healthchecks (`make up` verified)
- [x] `ingest` package with order book state machine
- [x] Produces to `market.raw.orderbook` with key `binance#{symbol}`
- [x] `ingest/Dockerfile` + `ingest` service in docker-compose (`make up` starts ingest automatically)
- [x] `make ingest` target for local dev (reads `.env`, connects to `localhost:19092`)
- [x] Unit tests: order book state machine (happy path, gap detection, level removal)
- [x] Integration test: produce → consume round-trip with testcontainers (skipped if no Docker)
- [x] `make lint typecheck test coverage` all pass

**Test coverage:** 102 tests, 89% total coverage.

**Note:** End-to-end data flow into Iceberg requires Phase 2 Flink jobs.

---

## Phase 2 — Flink Processing ✅

**Goal:** Normalized, deduplicated silver tables + OHLCV and liquidity metrics in real time.

**Done:** End-to-end flow verified 2026-05-15. `SELECT count(*) FROM iceberg.normalized.book_ticker` returns 176+ live rows with correct bid/ask/spread/mid_price. Both jobs stable in Flink UI.

### Jobs

**Job 1: `normalize.py`** — raw diffs → normalized book ticker

- Source: `market.raw.orderbook` (Kafka, DataStream `KafkaSource` + raw JSON)
- Sink: `normalized.book_ticker` (Iceberg) + `market.normalized.book_ticker` (Kafka)
- Dedup by `last_update_id`; late events (>30s) logged and dropped
- Computes: best bid/ask, spread, mid-price, top-5 imbalance

**Job 2: `ohlcv_1m.py`** — tumbling window aggregation

- Source: `market.normalized.book_ticker` (Kafka, Table API)
- Sink: `normalized.ohlcv_1m` (Iceberg)
- Window: 1 minute, event time
- Output: open, high, low, close, vwap, volume, trade_count

### Deliverables

- [x] Custom Flink Docker image: multi-stage build, Python 3.10, Kafka/Iceberg/Hadoop JARs, S3 plugin
- [x] `flink/jobs/lib/`: `config.py`, `sql_runner.py`, `logic.py` (pure Python, testable under 3.12)
- [x] SQL files: `sql/catalogs.sql`, `sql/normalize/*.sql`, `sql/ohlcv_1m/*.sql`
- [x] `normalize.py`: stateful `KeyedProcessFunction` + DataStream→Table bridge → Iceberg + Kafka sinks
- [x] `ohlcv_1m.py`: pure Table API, tumbling window OHLCV → Iceberg sink
- [x] docker-compose: `flink-jobmanager`, `flink-taskmanager`, `flink-init`; `AWS_REGION` configured
- [x] Unit tests: 17 sql_runner + 25 logic; `make lint typecheck test` all pass
- [x] `docs/DEBUGGING.md`: end-to-end verification runbook, failure table, reset procedure
- [x] `normalized.book_ticker` has live rows in Trino; both jobs RUNNING in Flink UI

### Key bugs fixed (reference)

| # | Error | Root Cause | Fix |
|---|---|---|---|
| 1 | `cannot import name 'UTC'` | `datetime.UTC` is Python 3.11+; Flink image is 3.10 | `UTC = timezone.utc`; suppress `UP017` in ruff per-file-ignores |
| 2 | `Cannot run program "python"` | Flink image has `python3` only | `ln -s /usr/bin/python3 /usr/bin/python` in Dockerfile |
| 3 | S3 JAR not found | Flink image is 1.18.**1**, not 1.18.0 | Use `flink-s3-fs-presto-*.jar` wildcard |
| 4 | `RestartStrategies` import error | Moved to `pyflink.common.restart_strategy` | Fix import path |
| 5 | `delay_between_attempts` kwarg error | PyFlink 1.18 uses `delay_interval` | Fix kwarg name |
| 6 | `CREATE CATALOG IF NOT EXISTS` parse error | Not supported in Flink 1.18 SQL | Remove `IF NOT EXISTS` |
| 7 | `ClassNotFoundException: hadoop.conf.Configuration` | Iceberg needs `hadoop-common` | Add `flink-shaded-hadoop-2-uber` JAR |
| 8 | `;` in SQL comments split statements | Naive `split()` on `;` | Rewrote as character-level parser |
| 9 | `LegacyTypeInformationType` cast error | `ARRAY<ARRAY<STRING>>` incompatible with DataStream bridge | Replace Table source with DataStream `KafkaSource` + raw JSON |
| 10 | `CustomPrint.print() flush` error | structlog calls `print(flush=True)`; PyFlink's Beam runner replaces `print()` | Remove structlog from Flink image; use standard `logging` |
| 11 | `ModuleNotFoundError: No module named 'structlog'` | cloudpickle serializes `OrderBookProcessor` with package references at submit time | Rebuild all three images together: jobmanager + taskmanager + flink-init |
| 12 | `Unable to load region from providers` | AWS SDK v2 ignores Flink/Iceberg config; reads only `AWS_REGION` env var | Add `AWS_REGION: us-east-1` to all three flink services in docker-compose |
| 13 | `No space left on device` (BLOB server) | Docker Desktop VM disk full after multiple `--no-cache` rebuilds | `docker system prune -f` |

### Known limitations

- `kafka_partition` and `kafka_offset` hardcoded to `-1` in normalize output — `SimpleStringSchema` doesn't expose Kafka record metadata. Fix: implement `KafkaRecordDeserializationSchema`.

---

## Phase 3 — CDC + Replay ✅

**Goal:** Postgres config data flowing via Debezium into Iceberg; Kafka offset replay works.

**Done:** End-to-end flow verified 2026-05-16. Debezium snapshot + live UPDATE upsert confirmed in Trino. See `docs/E2E_TESTING_PHASE3.md` for full runbook.

### 3a — Debezium CDC

- Source table: `symbol_config` in Postgres (pair metadata: status, lot size, tick size)
- Debezium connector config in `infra/config/debezium/`
- Kafka topic: `postgres.public.symbol_config`
- Flink CDC job: consume → handle op codes (r/c/u/d) → upsert `normalized.symbol_config`

### 3b — Replay producer

```
replay/src/replay/
  config.py
  iceberg_reader.py   Read raw Iceberg table to get kafka_offset range
  producer.py         Seek consumer to saved offset, re-produce to target topic
  main.py             CLI: --symbol --start-ts --end-ts --dry-run
```

**Replay protocol:**
1. Query `raw.orderbook_diffs` for offset range by time window
2. Optionally roll back silver Iceberg snapshot to pre-write state
3. Seek Kafka consumer to `start_offset`
4. Re-produce events; downstream dedup handles duplicates

**Deliverables:**
- [x] Debezium connector running, `postgres.public.symbol_config` topic populated
- [x] Flink CDC job: idempotent upsert into `normalized.symbol_config` (e2e verified)
- [x] `replay` CLI: unit-tested, dry-run mode implemented
- [ ] `make replay-sample` verified end-to-end against live stack
- [ ] Replay idempotency test: replay same range twice → same row count in Iceberg
- [ ] Iceberg snapshot rollback demonstrated in test or runbook
- [x] `make test` passes (95% coverage)

**Key bugs fixed during E2E:**

| # | Error | Root Cause | Fix |
|---|---|---|---|
| 1 | `manifest for debezium/connect:2.7 not found` | Tag 2.7 doesn't exist on Docker Hub | Change to `debezium/connect:2.6` |
| 2 | `Encountered ". raw" at line 1` | `raw` is a Flink SQL reserved keyword | Rename namespace `raw` → `bronze` in all SQL files |
| 3 | `debezium-init exit 22` on re-run | POST `/connectors` returns 409 if connector exists | Make debezium-init idempotent: check existence before POSTing |
| 4 | Normalized table not updating on UPDATE events | PostgreSQL DEFAULT REPLICA IDENTITY sends `before=null`; Flink debezium-json silently drops `op=u` records with null before | `ALTER TABLE symbol_config REPLICA IDENTITY FULL` |

---

## Phase 4 — Analytics Layer ✅

**Goal:** dbt models queryable via Trino; FastAPI serving OHLCV and liquidity endpoints; Prometheus + Grafana monitoring.

**Done:** Full stack e2e verified 2026-05-16. All 5 API endpoints return live Binance data. Grafana dashboard showing real-time prices, spreads, imbalance, and pipeline health. See `docs/DEBUGGING_PHASE4.md` for all gotchas.

### dbt models (9 models, PASS=6 in docker-compose)

```
staging (view):      stg_book_ticker, stg_ohlcv_1m, stg_symbol_config
intermediate (ephem): int_spread_metrics, int_order_book_imbalance, int_freshness_status
marts:               mart_ohlcv (table), mart_liquidity (view), mart_exchange_health (view)
```

Views recalculate `current_timestamp` on every Trino query → always fresh liquidity/health data without re-running dbt.

### FastAPI endpoints

```
GET /health                  liveness check
GET /ready                   readiness check (pings Trino)
GET /metrics                 Prometheus scrape endpoint
GET /ohlcv/{symbol}          1m OHLCV bars (last 60 by default, filterable by ts range)
GET /spread/{symbol}         best bid/ask, spread in bps
GET /liquidity/{symbol}      spread + imbalance + market signal + freshness
GET /pipeline/lag            health score + staleness for all 5 pairs
GET /symbols                 active trading pairs from CDC symbol_config
```

### Monitoring stack

- **Background poller**: FastAPI lifespan task queries Trino every 30s, updates Prometheus Gauges
- **Business metrics**: `market_mid_price_usd`, `market_spread_bps`, `market_bid_ask_imbalance`, `market_staleness_seconds`, `pipeline_health_score` — all labeled by `{symbol, exchange}`
- **Grafana dashboard**: 17 panels across 3 sections — API ops, Live Market Prices, Pipeline Health

### Key design decisions

**Freshness threshold: 60s not 30s** — Flink writes to Iceberg at checkpoint boundaries (~30–60s interval), not per-event. The data visible to Trino is always one checkpoint behind. Targeting `staleness ≤ 30s = FRESH` would cause health_score to oscillate 1.0↔0.5 every checkpoint cycle. The correct threshold is `≤ 60s` (one full checkpoint interval). See `docs/DEBUGGING_PHASE4.md`.

**Deliverables:**
- [x] 9 dbt models with `schema.yml` tests and source freshness
- [x] `dbt run` fully automated in docker-compose (`dbt-runner` waits for Flink schema then runs)
- [x] FastAPI with Pydantic request/response models on every endpoint
- [x] 21 integration tests, 91% coverage, mypy strict + ruff all pass
- [x] Prometheus middleware: `api_requests_total`, `api_request_duration_seconds`
- [x] Background market poller: 5 business Prometheus metrics updated every 30s
- [x] Grafana: 17-panel dashboard provisioned via config file (no manual setup)
- [x] `api/Dockerfile`: `python:3.13-slim` + curl installed for healthcheck
- [x] `docs/MARKET_CONCEPTS.md` + `MARKET_CONCEPTS_ZH.md`: bilingual market microstructure glossary

**Done when:** `curl localhost:8000/ohlcv/btcusdt` returns live K-line data ✓

---

## Phase 5 — Ops + Observability

**Goal:** Airflow orchestration, data quality checks, freshness SLA alerts.

### Airflow DAGs

```
backfill_ohlcv_1m          Spark: recompute OHLCV for a date range (idempotent)
compact_iceberg_tables      Spark: rewrite_data_files nightly
expire_iceberg_snapshots    Retain last 7 days of snapshots
run_dbt_models              dbt run + dbt test
run_great_expectations      Row counts, nulls, price range checks
freshness_sla_check         Alert if any symbol > 35s stale
```

### Great Expectations checks (per table)

- `exchange_event_ts` not null
- `price` > 0, `volume` ≥ 0
- Row count within expected range per `(exchange, symbol, date)`
- Freshness: `max(exchange_event_ts) > now() - 35s`

**Deliverables:**
- [ ] 6 Airflow DAGs, all idempotent, all with exponential-backoff retries
- [ ] Spark compaction job for `normalized.book_ticker`
- [ ] Great Expectations suite with checkpoint
- [ ] Freshness alert fires when a symbol goes stale (can simulate by stopping ingest)
- [ ] `make test` passes

**Done when:** Airflow UI shows all DAGs green on a nightly run.

---

## Phase 6 — Demo + Blog + Landing Page

**Goal:** Record a polished demo, publish blog posts, launch ticksense.ai.

### Demo recording checklist

- [ ] `make up` → all services green
- [ ] Ingest running: data flowing into Redpanda → Iceberg
- [ ] Flink UI: 2 jobs running, no restarts, lag < 1s
- [ ] Trino query: `SELECT * FROM iceberg.normalized.ohlcv_1m LIMIT 10`
- [ ] FastAPI: live OHLCV response in browser
- [ ] Replay demo: stop ingest → run replay → row count recovers
- [ ] Airflow: trigger backfill DAG, show task success

### Blog posts (suggested sequence)

1. **Architecture overview** — why lakehouse, why Iceberg, why Flink
2. **L2 order book deep dive** — snapshot + diff protocol, state machine, gap handling
3. **Exactly-once semantics** — Flink checkpointing + Iceberg two-phase commit
4. **CDC with Debezium** — op codes, upsert pattern, replay protocol
5. **dbt over Trino** — modeling lakehouse data, freshness tests

### Landing page (ticksense.ai)

- Tech: Nuxt 3 + TypeScript + Tailwind CSS v4
- Deploy: Vercel free tier + custom domain
- Sections: hero, architecture diagram, key metrics, tech stack, links
- Links to: GitHub, Bilibili/YouTube demo, Medium/blog posts
- Repo: separate `ticksense-web/` repository

**Done when:** ticksense.ai resolves, demo video is published, at least one blog post is live.

---

## Phase 7 — AI Agent (Future)

Not in scope until Phase 6 is complete.

Ideas:
- LLM-powered signal commentary ("BTC spread widened 3x in the last 5 minutes — large sell wall appeared at $67,200")
- RAG over blog posts + financial news for context
- Real-time alerts via Telegram / WeChat

---

## Optional / Good to Have

These are not on the critical path but worth revisiting as the ecosystem matures.

### Migrate from mypy to ty

[ty](https://github.com/astral-sh/ty) is Astral's Rust-based type checker — the third piece of the ruff/uv/ty trilogy, significantly faster than mypy.

**Blocked by:** ty does not support mypy plugins. This project uses `plugins = ["pydantic.mypy"]` in `pyproject.toml`. Pydantic v2 has much better native type checker support than v1, so the plugin may be droppable once ty matures enough to cover the edge cases (e.g. `model_validator`, `TypeAdapter` generics under `strict = true`).

**Migration steps (when ready):**
1. Remove `plugins = ["pydantic.mypy"]` from `[tool.mypy]`
2. Replace `mypy` with `ty` in the root dev dependency group
3. Replace `uv run mypy ingest/src api/src replay/src` with `uv run ty check ingest/src api/src replay/src` in `Makefile` and `.github/workflows/ci.yml`
4. Fix any new type errors surfaced by ty

---

## Dependency graph

```
Phase 0 (done)
    └── Phase 1 (docker-compose + ingest)
            └── Phase 2 (Flink processing)
                    ├── Phase 3 (CDC + replay)
                    └── Phase 4 (dbt + API)
                            └── Phase 5 (Airflow + ops)
                                    └── Phase 6 (demo + blog + web)
                                            └── Phase 7 (AI, future)
```

Each phase builds on the previous. Phases 3 and 4 can overlap once Phase 2 is stable.
