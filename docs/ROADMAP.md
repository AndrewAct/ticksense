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

## Phase 1 — Local Stack + Ingestion

**Goal:** Binance L2 order book data flowing from WebSocket into raw Iceberg tables.

### 1a — docker-compose.yml

| Service | Image | Purpose |
|---|---|---|
| redpanda | vectorized/redpanda | Kafka broker |
| redpanda-console | redpandadata/console | Topic browser |
| postgres | postgres:16 | OLTP + Airflow backend |
| minio | minio/minio | S3-compatible object store |
| iceberg-rest | tabulario/iceberg-rest | Iceberg REST catalog |
| trino | trinodb/trino | Query engine |
| flink-jobmanager | flink:1.18 | PyFlink job manager |
| flink-taskmanager | flink:1.18 | PyFlink task manager |
| debezium | debezium/connect | CDC (added in Phase 3) |

`make up` starts all services and blocks until healthchecks pass.

### 1b — ingest package

```
ingest/src/ingest/
  config.py          Settings via pydantic-settings
  schemas.py         Pydantic models: L2Diff, OrderBookSnapshot, MarketEvent
  orderbook.py       Order book state machine (snapshot + diff maintenance)
  binance/
    websocket.py     Async WebSocket client, reconnect logic
    rest.py          Snapshot fetch (GET /api/v3/depth)
  producer.py        aiokafka producer, idempotent, DLQ routing
  main.py            Entry point: connect → maintain book → produce
```

**Key behaviors:**
- On startup: REST snapshot → buffer WebSocket diffs → apply in order
- On sequence gap: re-fetch snapshot, reset state for that symbol
- On serialization error: route to `market.dlq`, log, continue
- Graceful shutdown: flush producer before exit

**Deliverables:**
- [ ] `docker-compose.yml` with all services + healthchecks
- [ ] `ingest` package with order book state machine
- [ ] Produces to `market.raw.orderbook` with key `binance#btcusdt`
- [ ] Raw Iceberg table `raw.orderbook_diffs` created and receiving data
- [ ] Unit tests: order book state machine (happy path, gap detection, level removal)
- [ ] Integration test: produce → consume round-trip with testcontainers
- [ ] `make up && make test` passes

**Done when:** `SELECT count(*) FROM iceberg.raw.orderbook_diffs` returns > 0 after 60s.

---

## Phase 2 — Flink Processing

**Goal:** Normalized, deduplicated silver tables + OHLCV and liquidity metrics in real time.

### Jobs

**Job 1: `normalize.py`** — raw diffs → normalized book ticker

- Source: `market.raw.orderbook` (Kafka)
- Sink: `normalized.book_ticker` (Iceberg)
- Dedup by `last_update_id`
- Compute: best bid, best ask, spread, mid-price, top-5 imbalance
- Late events (>30s) → `market.dlq`

**Job 2: `ohlcv_1m.py`** — tumbling window aggregation

- Source: `normalized.book_ticker`
- Sink: `normalized.ohlcv_1m`
- Window: 1 minute, event time
- Output: open, high, low, close, vwap, volume, trade_count

**Deliverables:**
- [ ] PyFlink job: normalize + dedup + spread/imbalance computation
- [ ] PyFlink job: 1m OHLCV tumbling window
- [ ] Iceberg tables: `normalized.book_ticker`, `normalized.ohlcv_1m`, `normalized.liquidity_metrics`
- [ ] All jobs: checkpoint every 60s, restart strategy configured, DLQ side output
- [ ] Flink UI (localhost:8081) shows running jobs with no restarts
- [ ] Unit tests: window logic, dedup key generation, imbalance formula
- [ ] `make test` passes

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
