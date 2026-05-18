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
