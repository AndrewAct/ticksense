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
- Graceful shutdown: flush producer before exit

**Deliverables:**
- [x] `docker-compose.yml` with all services + healthchecks (`make up` verified)
- [x] `ingest` package with order book state machine
- [x] Produces to `market.raw.orderbook` with key `binance#{symbol}`
- [x] Unit tests: order book state machine (happy path, gap detection, level removal)
- [x] Integration test: produce → consume round-trip with testcontainers (skipped if no Docker)
- [x] `make lint typecheck test coverage` all pass

**Test coverage:** 57 tests, 96% coverage.

**Note:** `raw.orderbook_diffs` Iceberg table and end-to-end data flow require Phase 2 Flink jobs.

**Done when:** `SELECT count(*) FROM iceberg.raw.orderbook_diffs` returns > 0 after 60s. *(Phase 2)*

---

## Phase 2 — Flink Processing 🚧 In Progress

**Goal:** Normalized, deduplicated silver tables + OHLCV and liquidity metrics in real time.

### Jobs

**Job 1: `normalize.py`** — raw diffs → normalized book ticker

- Source: `market.raw.orderbook` (Kafka, via DataStream `KafkaSource` + raw JSON — see bug #9 below)
- Sink: `normalized.book_ticker` (Iceberg) + `market.normalized.book_ticker` (Kafka) + `market.dlq` (DLQ)
- Dedup by `last_update_id`
- Compute: best bid, best ask, spread, mid-price, top-5 imbalance
- Late events (>30s) → `market.dlq`

**Job 2: `ohlcv_1m.py`** — tumbling window aggregation

- Source: `market.normalized.book_ticker` (Kafka, Table API)
- Sink: `normalized.ohlcv_1m` (Iceberg)
- Window: 1 minute, event time
- Output: open, high, low, close, vwap, volume, trade_count

### Implemented

- [x] Custom Flink Docker image: multi-stage build (JDK headers graft + python3 + pemja + connector JARs)
- [x] `flink/jobs/lib/config.py` — settings class with `as_dict()` for SQL template substitution
- [x] `flink/jobs/lib/sql_runner.py` — SQL file runner: `load_sql`, `split_statements`, `execute_sql_file`, `add_inserts_from_file`
- [x] `flink/jobs/lib/logic.py` — pure Python order book logic (no PyFlink, fully testable under Python 3.12)
- [x] SQL files: `sql/catalogs.sql`, `sql/normalize/*.sql`, `sql/ohlcv_1m/*.sql`
- [x] `normalize.py` — DataStream `KafkaSource` source + stateful `KeyedProcessFunction` + DataStream→Table bridge
- [x] `ohlcv_1m.py` — pure Table API, tumbling window OHLCV
- [x] Unit tests: 17 tests for `sql_runner`, 25+ tests for `logic`
- [x] docker-compose: `flink-jobmanager`, `flink-taskmanager` (custom image), `flink-init` job submitter
- [x] `normalize.py` job **RUNNING** in Flink (JobID: ef2f3875...) as of 2026-05-15

### Deliverables Status

- [x] PyFlink job: normalize + dedup + spread/imbalance computation
- [x] PyFlink job: 1m OHLCV tumbling window (code complete, submission pending)
- [x] Iceberg tables: DDL for `normalized.book_ticker`, `normalized.ohlcv_1m`
- [x] All jobs: checkpoint every 60s, restart strategy configured, DLQ side output
- [~] Flink UI (localhost:8081): `normalize` RUNNING — `ohlcv_1m` submission not yet confirmed
- [x] Unit tests: sql_runner, logic
- [ ] `make test` passes end-to-end
- [ ] `normalized.ohlcv_1m` has rows for each symbol (need ingest running)

### Bugs Encountered and Fixed (for reference)

| # | Error | Root Cause | Fix |
|---|---|---|---|
| 1 | `cannot import name 'UTC' from 'datetime'` | `datetime.UTC` added in Python 3.11; Flink image has Python 3.10 | `from datetime import timezone; UTC = timezone.utc` |
| 2 | `Cannot run program "python"` | Flink image only has `python3`, not `python` | `ln -s /usr/bin/python3 /usr/bin/python` in Dockerfile |
| 3 | `cp flink-s3-fs-presto-1.18.0.jar: No such file` | Flink image is actually 1.18.**1**, not 1.18.0 | Use `flink-s3-fs-presto-*.jar` wildcard |
| 4 | `cannot import name 'RestartStrategies' from 'pyflink.datastream'` | `RestartStrategies` is in `pyflink.common.restart_strategy`, not re-exported by `pyflink.datastream` | `from pyflink.common.restart_strategy import RestartStrategies` |
| 5 | `TypeError: failure_rate_restart() got unexpected keyword argument 'delay_between_attempts'` | Actual kwarg is `delay_interval` in PyFlink 1.18 bundled version | Change kwarg name |
| 6 | `SQL parse failed. Encountered "NOT" at line 4` | `CREATE CATALOG IF NOT EXISTS` not supported in Flink 1.18 (IF NOT EXISTS for CREATE CATALOG was added later) | Remove `IF NOT EXISTS` — catalog is session-scoped and always created fresh |
| 7 | `ClassNotFoundException: org.apache.hadoop.conf.Configuration` | Iceberg's `FlinkCatalogFactory` always calls `clusterHadoopConf()` which needs `hadoop-common`, not included in base Flink image | Add `flink-shaded-hadoop-2-uber-2.8.3-10.0.jar` to Dockerfile |
| 8 | `Non-query expression encountered in illegal context` (sink_iceberg.sql) | `split_statements()` naively split on `;`, including semicolons inside `--` comments (line 6 of sink_iceberg.sql has `; add DAY(...)`) | Rewrote `split_statements` as proper character-level parser: skips `;` inside `--` line comments, `/* */` block comments, and string literals |
| 9 | `LegacyTypeInformationType cannot be cast to ArrayType` | `to_data_stream()` on a table with `ARRAY<ARRAY<STRING>>` columns (bids/asks) fails in PyFlink 1.18 — these map to `LegacyTypeInformationType` which can't be serialized through the DataStream bridge | Replaced Table API source + `to_data_stream()` with DataStream `KafkaSource` + `SimpleStringSchema`. `OrderBookProcessor` now parses raw JSON strings. `source.sql` kept for documentation but no longer executed. |

### Known Limitations / TODO for Next Session

1. **kafka_partition and kafka_offset are hardcoded to `-1`** in normalize output — `SimpleStringSchema` doesn't expose Kafka record metadata. To fix properly: implement a custom `KafkaRecordDeserializationSchema` in Java or use a different source approach.

2. **`ohlcv_1m.py` submission status unknown** — was being submitted when session ended. Need to verify it started successfully in Flink UI at localhost:8081. If it failed, need to check logs with `docker compose logs flink-init`.

3. **Need ingest service running** to actually produce data through the pipeline and verify `normalized.ohlcv_1m` gets rows.

4. **`source.sql` no longer used by `normalize.py`** — kept as documentation of the raw orderbook schema, but not executed at runtime. Consider adding a comment to the file noting this.

5. **`pyproject.toml` workspace members** — `flink/` is NOT a uv workspace member (PyFlink runs under Python 3.10 in Docker, the uv workspace uses Python 3.12). Verify `members = ["ingest", "api", "replay"]` — `"flink"` should NOT be present.

**Done when:** 5 minutes after `make up`, `normalized.ohlcv_1m` has rows for each symbol.

---

## Phase 3 — CDC + Replay

**Goal:** Postgres config data flowing via Debezium into Iceberg; Kafka offset replay works.

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
- [ ] Debezium connector running, `postgres.public.symbol_config` topic populated
- [ ] Flink CDC job: idempotent upsert into `normalized.symbol_config`
- [ ] `replay` CLI: `make replay-sample` produces 10k events dry-run
- [ ] Replay idempotency test: replay same range twice → same row count in Iceberg
- [ ] Iceberg snapshot rollback demonstrated in test or runbook
- [ ] `make test` passes

**Done when:** `replay-sample` completes without error; re-running it doesn't change row count.

---

## Phase 4 — Analytics Layer

**Goal:** dbt models queryable via Trino; FastAPI serving OHLCV and liquidity endpoints.

### dbt models

```
staging:    stg_orderbook_diffs, stg_book_ticker, stg_symbol_config
intermediate: int_ohlcv_1m, int_spread_metrics, int_order_book_imbalance,
              int_liquidity_score, int_freshness_status
marts:      mart_ohlcv, mart_liquidity, mart_volatility, mart_exchange_health
```

### FastAPI endpoints

```
GET /health
GET /ready
GET /metrics
GET /v1/ohlcv?symbol=BTCUSDT&interval=1m&limit=100
GET /v1/liquidity?symbol=BTCUSDT
GET /v1/symbols
GET /v1/freshness
```

**Deliverables:**
- [ ] 12 dbt models with schema.yml tests and source freshness
- [ ] `make dbt-run && make dbt-test` passes
- [ ] FastAPI service with Pydantic request/response models on every endpoint
- [ ] `/health`, `/ready`, `/metrics` on FastAPI
- [ ] Integration tests: FastAPI → Trino → Iceberg round-trip
- [ ] `make test` passes

**Done when:** `curl localhost:8000/v1/ohlcv?symbol=BTCUSDT&interval=1m` returns data.

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
