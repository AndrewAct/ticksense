# TickSense — Phase 5 Debug Runbook (Airflow + Spark + Kafka Offset Fix)

---

## Flink

### `ImportError: cannot import name 'KafkaRecordDeserializer' from 'flink_lib.kafka_schema'`

**Symptom:**

```
=== Submitting normalize job ===
Traceback (most recent call last):
  File "/opt/flink/jobs/normalize.py", line 35, in <module>
    from flink_lib.kafka_schema import (
ImportError: cannot import name 'KafkaRecordDeserializer' from 'flink_lib.kafka_schema'
org.apache.flink.client.program.ProgramAbortException: ...
  Python process exits with code: 1
```

`flink-init` exits with code 31. This cascades: `dbt-runner` depends on `flink-init: service_completed_successfully`, so it never starts. `api` depends on `dbt-runner`, `prometheus` depends on `api`, `grafana` depends on `prometheus` — all four end up in Docker "Created" (never started) state.

**Root cause (two-layer):**

1. `KafkaRecordDeserializer` inherits from `KafkaRecordDeserializationSchema`. That class exists in the Flink Java API but **is not exposed in PyFlink 1.18 Python bindings**. The `try/except ImportError: pass` in `kafka_schema.py` silently swallows the import failure, so `KafkaRecordDeserializer` is never defined.

2. Even if it were importable, `KafkaSourceBuilder` in PyFlink 1.18 only exposes `set_value_only_deserializer()` — there is no `set_deserializer()` method at all.

**Verification:**

```bash
docker run --rm ticksense-flink-jobmanager python3 -c \
  "from pyflink.datastream.connectors.kafka import KafkaRecordDeserializationSchema"
# ImportError: cannot import name 'KafkaRecordDeserializationSchema' ...

docker run --rm ticksense-flink-jobmanager python3 -c \
  "from pyflink.datastream.connectors.kafka import KafkaSource; \
   print([m for m in dir(KafkaSource.builder()) if not m.startswith('_')])"
# [..., 'set_value_only_deserializer']   ← no set_deserializer
```

**Fix:** Replace the `KafkaSource.builder()` DataStream approach with a Flink SQL Table API source that exposes Kafka metadata columns directly. Real partition and offset are captured via `METADATA FROM 'partition' VIRTUAL` and `METADATA FROM 'offset' VIRTUAL` columns, then bridged to DataStream via `t_env.to_append_stream()`.

New SQL source table pattern (used in `normalize/source_raw.sql` and `cdc_symbol_config/source_bronze.sql`):

```sql
CREATE TEMPORARY TABLE raw_orderbook_stream (
    payload         STRING,
    kafka_partition INT     METADATA FROM 'partition' VIRTUAL,
    kafka_offset    BIGINT  METADATA FROM 'offset'    VIRTUAL
) WITH (
    'connector'  = 'kafka',
    'topic'      = '{topic_raw}',
    ...
    'format'     = 'raw'
)
```

In Python:

```python
execute_sql_file(t_env, SQL / "normalize" / "source_raw.sql", **cfg.as_dict())
raw_stream = t_env.to_append_stream(
    t_env.sql_query(
        "SELECT payload, kafka_partition, kafka_offset FROM raw_orderbook_stream"
    ),
    KAFKA_RECORD_TYPE,
)
```

`KafkaRecordDeserializer` class removed from `flink_lib/kafka_schema.py` entirely.

---

### Why `METADATA FROM 'partition' VIRTUAL` and not just `METADATA FROM 'partition'`

`VIRTUAL` marks the column as **read-only metadata**. Without it, Flink treats the column as both readable and writable. On a source-only table this doesn't matter functionally, but omitting `VIRTUAL` on a metadata column that Kafka controls (partition assignment is done by the broker, not the producer) is misleading and would cause errors if the table were ever used as a sink.

The value is populated automatically by the Flink Kafka connector: when Flink reads each `ConsumerRecord`, the connector extracts `record.partition()` and `record.offset()` from the Kafka metadata and fills the declared columns. No user code required.

**Mental model:** Same as PostgreSQL's `ctid` or a database `ROWID` — the system fills it, you can read it, you cannot write it.

---

### `format = 'raw'` vs `format = 'json'` for the DataStream source

The existing `normalize/source.sql` uses `format = 'json'` and declares `bids ARRAY<ARRAY<STRING>>` and `asks ARRAY<ARRAY<STRING>>`. In PyFlink 1.18, `ARRAY<ARRAY<STRING>>` cannot be bridged through `to_append_stream()` — it triggers a `LegacyTypeInformationType` cast error (documented in Phase 2 bugs).

`format = 'raw'` reads the entire Kafka message value as a single `STRING`. The table must have exactly one non-metadata column of type `STRING` or `BYTES`. The resulting DataStream emits `Row(payload, kafka_partition, kafka_offset)` with no complex nested types.

The `OrderBookProcessor` already parses the JSON payload in Python, so it does not need the structured fields from the SQL layer. The `source.sql` table (with full JSON schema) is kept for potential future Table-API-only paths but is not used in the DataStream job.

---

### Phase 5 lib namespace conflict — `lib/kafka_schema.py` not deleted after rename

**Symptom:** Even after Phase 5 renamed `flink/jobs/lib/` to `flink/jobs/flink_lib/`, a stale `flink/jobs/lib/kafka_schema.py` survived in the git tree.

**Root cause:** The merge commit (`bb45f4f`) handled the rename correctly for `__init__.py`, `config.py`, `logic.py`, and `sql_runner.py` (shown as `{lib => flink_lib}/file` in the diff), but `kafka_schema.py` was added as a new file to `flink_lib/` **without deleting** the old `lib/kafka_schema.py`. Git `ls-files flink/jobs/lib/` confirmed the file was still tracked.

**Effect:** Two copies of the module existed in the Docker image:
- `/opt/flink/jobs/flink_lib/kafka_schema.py` (correct, used)
- `/opt/flink/jobs/lib/kafka_schema.py` (stale, harmless on its own but confusing)

**Fix:** `git rm flink/jobs/lib/kafka_schema.py`.

---

### Container cascade from flink-init failure

When `flink-init` exits with a non-zero code, Docker Compose does not start any service whose `depends_on` specifies `condition: service_completed_successfully` for `flink-init`. The full cascade in this project:

```
flink-init (Exit 31)
  └─ dbt-runner (Created — never started)
       └─ api (Created — never started)
            └─ prometheus (Created — never started)
                 └─ grafana (Created — never started)
```

Diagnosis: look for exit code in `docker ps -a`. "Created" (not "Exited") on downstream containers is the tell.

**Rule:** Fix the flink job submission error first. Everything else unblocks automatically.

---

## Docker

### `__pycache__` directories copied into Docker images

**Symptom:** After `make build`, running `docker run --rm <image> find /opt/flink/jobs -name "__pycache__"` shows bytecode directories from the host machine.

**Root cause:** No `.dockerignore` file. The `COPY jobs/ /opt/flink/jobs/` instruction copies everything, including `__pycache__/` directories containing `.pyc` files compiled by the host Python (3.13). The Flink container runs Python 3.10. Python detects the version mismatch and ignores the `.pyc` files, but they bloat the image and are confusing.

Similarly, the `ingest` and `api` images use `context: .` (project root) with `COPY ingest/ ./ingest/` and `COPY api/ ./api/`, pulling in test `__pycache__` directories.

**Fix:**

`flink/.dockerignore`:
```
**/__pycache__
**/*.pyc
**/*.pyo
tests/
```

`.dockerignore` (root, for `ingest` and `api` builds):
```
**/__pycache__
**/*.pyc
**/*.pyo
.git/
```

**Verification:**
```bash
docker run --rm ticksense-flink-init find /opt/flink/jobs -name "__pycache__" -type d
# (no output — clean)
```

**Note:** Each build context needs its own `.dockerignore`. The root `.dockerignore` is read when the build context is `.`. The `flink/.dockerignore` is read when the build context is `./flink`. They do not inherit from each other.

---

## Airflow (WIP — pending E2E test)

The Airflow DAGs and Docker image are implemented but have not yet been run end-to-end against the live stack. Notes below are based on code review only.

### DAG structure

| DAG | Schedule | Purpose |
|---|---|---|
| `backfill_ohlcv_1m` | manual / on-demand | Spark: recompute OHLCV for a date range |
| `compact_iceberg_tables` | `0 2 * * *` | Spark: `rewrite_data_files` on bronze + normalized tables |
| `expire_iceberg_snapshots` | `0 3 * * *` | Retain last 7 days; older snapshots deleted |
| `run_dbt_models` | `0 4 * * *` | `dbt run --full-refresh` on mart models |
| `run_great_expectations` | `0 5 * * *` | Row count, null check, price range validation |
| `freshness_sla_check` | `*/5 * * * *` | Alert if any symbol > 35s stale |

### Known risks to test

- `backfill_ohlcv_1m` passes date-range params to Spark via `SparkSubmitOperator`. Verify param passing and idempotency (re-running same date range should produce same result, not duplicates).
- `compact_iceberg_tables` uses `rewrite_data_files` with a `min_file_size_bytes` threshold. Verify it doesn't compact files that are actively being written by Flink.
- `freshness_sla_check` queries Trino. Verify the Trino connection string works from inside the Airflow container network.

---

## Spark (WIP — pending E2E test)

The Spark jobs are implemented but have not yet been submitted against the live stack.

### Job structure

| Job | Trigger | Purpose |
|---|---|---|
| `backfill_ohlcv.py` | Airflow / CLI | Recompute 1m OHLCV from bronze for a given date range |
| `compact_tables.py` | Airflow | `rewrite_data_files` on Iceberg tables |

### `spark_lib/` package

- `config.py`: reads env vars (`ICEBERG_REST_URI`, `S3_ENDPOINT`, `AWS_*`, date range params)
- `sql_runner.py`: minimal SQL file execution helper (mirrors `flink_lib/sql_runner.py` pattern)

### Why `spark_lib/` not `lib/`

Phase 5 originally used a bare `lib/` directory in both `flink/jobs/` and `spark/jobs/`, which created a Python namespace package collision when both were on `sys.path`. Renamed to `flink_lib/` and `spark_lib/` respectively to give each service a unique package name.

### Known risks to test

- Spark job uses `IcebergSparkSessionExtensions` and `SparkCatalog`. Verify catalog config matches the REST catalog endpoint and S3 credentials.
- `backfill_ohlcv.py` writes to `normalized.ohlcv_1m` which Flink also writes to. Test for concurrent write conflicts (Iceberg uses optimistic concurrency — conflicts result in retries, not data loss, but worth verifying).
- SQL files (`backfill_ohlcv.sql`, `compact_table.sql`) use Iceberg `CALL` procedures. Verify these are supported by the installed `iceberg-spark-runtime` JAR version.

---

## Port reference

| Service | Host port |
|---|---|
| FastAPI | 8000 |
| Prometheus | 9090 |
| Grafana | 3000 |
| Trino | 8082 |
| MinIO UI | 9001 |
| Flink UI | 8081 |
| Redpanda Console | 8080 |
| Debezium | 8083 |

---

## Load Test — k6 OHLCV 100% 404 + High Latency Diagnosis (2026-05-17)

### Symptom

k6 load test (10 VUs, 3.5 min ramp-up profile) shows:
- `ohlcv 200`: 0% — all 409 requests return 404
- `http_req_failed`: 16.66% (= 409 failed / 2454 total — all failures are from ohlcv)
- `errors`: 0.00% — no 5xx errors
- p(95) latency: 149ms at only 10 VUs

### Root cause 1 — OHLCV 404 (data pipeline gap)

`iceberg.marts.mart_ohlcv` is empty. The API correctly returns 404 when no rows exist.

The chain is: `Flink ohlcv_1m.py → iceberg_cat.normalized.ohlcv_1m → dbt stg_ohlcv_1m → dbt mart_ohlcv → API`.

If either step is missing, the mart is empty:
1. The Flink OHLCV job (`flink/jobs/ohlcv_1m.py`) was not running during the test
2. `dbt run` was not executed after Flink started writing to `normalized.ohlcv_1m`

The distinguishing signal is `errors: 0%` — if the table didn't exist at all, Trino would throw an exception → the API would return 500 → errors would be non-zero. 0% errors + 16.66% http_req_failed = 404 = table exists, data absent.

**Fix:** Ensure the Flink OHLCV job is running before load testing, and that `dbt run` has been executed. In docker-compose, `dbt-runner` runs automatically after Flink initialises the schema.

### Root cause 2 — Latency (no caching + new connection per request)

Three stacked problems:
1. `TrinoClient._connect()` opens a new TCP connection to Trino on **every request** (connection setup ~20-50ms overhead even before the query executes)
2. Trino has a ~50-100ms minimum query latency floor per query due to distributed query planning overhead — even `WHERE symbol = X LIMIT 60` pays this cost
3. Zero response caching: OHLCV bars change at most every 60s (one checkpoint), liquidity every ~5s, but every API request hits Trino cold

At 10 VUs: p(95)=149ms. At 100+ VUs: Trino connection storm pushes latency past 500ms.

**Fix applied (2026-05-17):**
- `api/src/api/dependencies.py`: `get_client()` decorated with `@lru_cache(maxsize=1)` → singleton TrinoClient
- `api/src/api/routers/ohlcv.py`: `TTLCache(maxsize=500, ttl=60)` keyed by `(symbol, limit, from_ts, to_ts)` — 60s TTL aligns with the 1-minute bar interval; cached hits are microsecond-fast
- `api/src/api/routers/liquidity.py`: `TTLCache(maxsize=200, ttl=5)` for spread and liquidity — 5s TTL matches order book update cadence
- `api/tests/integration/test_endpoints.py`: `autouse` fixture clears module-level caches before each test to prevent cross-test cache hits poisoning `mock.fetch.call_args`
- `api/pyproject.toml`: added `cachetools>=5.3` dependency

**Thread safety note:** `cachetools.TTLCache` is not thread-safe. All cache reads/writes are protected with a `threading.Lock`. This matters because `anyio.to_thread.run_sync` runs Trino calls in a thread pool — without the lock, concurrent requests can corrupt the cache.

### Poller-as-read-model (2026-05-17)

The next architectural step was removing Trino entirely from the hot request path.

**Before:** Every API request → TTL cache check → Trino query (on cache miss)
**After:** Every API request → in-process `ReadModel` dict lookup (<1 µs). Trino is only called by the background poller on its refresh schedule.

**`api/src/api/read_model.py`** — New module. `ReadModel` dataclass holds:
- `spread: dict[str, SpreadResponse]` and `liquidity: dict[str, LiquidityResponse]` — keyed by uppercase symbol
- `ohlcv: dict[str, OHLCVResponse]` — last 60 bars per symbol, newest-first
- `pipeline: PipelineLagResponse | None`
- `symbols: SymbolsResponse | None`
- `ready: bool` — False until first full poll cycle completes

**`poller.py`** — Full rewrite. Replaces the old single-query poller with four separate refresh functions, each called on its own cadence:
- `_poll_liquidity` / `_poll_pipeline`: every 30 s, also update Prometheus gauges
- `_poll_ohlcv`: every 60 s, single window-function query for ALL symbols
- `_poll_symbols`: every 300 s (CDC data rarely changes)

OHLCV SQL uses `ROW_NUMBER() OVER (PARTITION BY symbol ORDER BY window_start DESC)` to get the 60 most-recent bars per symbol in one round trip, bounded to `WHERE window_start >= NOW() - INTERVAL '2' HOUR` to keep the scan cheap.

**Endpoint routing:**
- Spread / liquidity / pipeline / symbols: read from `ReadModel`, no Trino at all
- OHLCV default (`limit ≤ 60`, no time filter): read from `ReadModel`, sub-millisecond
- OHLCV historical (`from_ts` / `to_ts` set, or `limit > 60`): cold path to Trino, with `TTLCache(60 s)` so repeated analytical queries don't pound Trino

**Cold-start (before first poll completes):** endpoints return `503 Service Warming Up`. The initial poll runs immediately in `run_poller()` via `asyncio.gather(return_exceptions=True)` — in practice the warm-up window is < 1 s on a healthy stack.

**Thread-safety:** `ReadModel` fields are updated by swapping entire dicts (`_model.spread = new_dict`). Under CPython's GIL, dict reference assignment is atomic — request handlers either see the old dict or the new one, never a partial update. The `TTLCache` on the cold path still uses a `threading.Lock` as before.

---

## Load Test — Symbol Case Bug: spread/liquidity 100% 404 Despite Mart Having Data (2026-05-18)

### Symptom

After the ReadModel architecture was deployed (p(95)=3 ms confirmed), a second load test showed:
- `spread 200` ↳ 0% (all 404)
- `liquidity 200` ↳ 0% (all 404)
- `ohlcv 200` ↳ 0% (all 404)

Yet `mart_liquidity` had 5 rows and `poll_liquidity_ok symbols=5` appeared in the API logs.

### Root cause 1 — Symbol case mismatch (spread + liquidity)

`mart_liquidity` stores symbols in **lowercase** (`btcusdt`, `ethusdt`, …). The poller built the ReadModel dict with lowercase keys:

```python
sym = str(r["symbol"])          # "btcusdt"
new_spread[sym] = ...           # keyed "btcusdt"
```

But the router looked up with `.upper()`:

```python
sym = symbol.upper()            # "BTCUSDT"
model.spread.get(sym)           # None → 404
```

The unit tests passed because the test fixture directly populated the model with `"BTCUSDT"` (uppercase), matching what the router expected. The mismatch only appeared in production where the mart uses lowercase.

**Fix:** `sym = str(r["symbol"]).upper()` in both `_poll_liquidity` and `_poll_ohlcv` in `poller.py`.

**Rule:** Always normalize symbol keys to uppercase at the point where the ReadModel dict is built, not at the lookup site. That way, routers can use `.upper()` without coupling to the mart's storage convention.

### Root cause 2 — mart_ohlcv empty (dbt timing)

`mart_ohlcv` had 0 rows even though Flink was running. The cause:

```
make up
  → flink-init submits Flink jobs (~30 s)
  → dbt-runner runs dbt IMMEDIATELY after flink-init exits
      At this point Flink has not completed a single checkpoint.
      normalized.ohlcv_1m is empty → mart_ohlcv is empty.
  → api starts, poller polls → poll_ohlcv_ok symbols=0
```

Docker Compose `depends_on: service_completed_successfully` only waits for the service to *exit*, not for the system it triggered to *produce data*. The dbt-runner exits before Flink writes its first row.

**Fix:** After `make up`, wait ~60 s for Flink to checkpoint, then run `make dbt-run` manually. The `make load-test-full` target automates this:

1. Waits until 2 Flink jobs are `RUNNING`
2. Waits until `normalized.book_ticker` has rows (proxy for first checkpoint)
3. Runs `dbt run`
4. Verifies `mart_liquidity` has rows
5. Runs k6

**Rule:** Never assume `dbt-runner` produced fresh marts. It runs once at startup under ideal conditions (stack just started, no data yet). For any load test or manual verification, run `make dbt-run` after the pipeline has been flowing for at least one minute.
