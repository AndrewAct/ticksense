---
name: flink-stream-processing
description: Use when implementing or modifying PyFlink jobs, watermarks, stateful operators, window aggregations, or Iceberg sinks.
---

# Flink Stream Processing

## Before writing a job, document all of these

| Question | Must answer |
|---|---|
| Source topic(s) | |
| Sink table / topic | |
| Event time field | |
| Watermark strategy + allowed lateness | |
| Dedup key | |
| State backend + TTL | |
| Checkpoint interval | |
| Restart strategy | |
| Late event handling | |
| DLQ side output | |

## TickSense defaults

- Event time field: `exchange_event_ts` for all market data
- Watermark: bounded out-of-orderness, 5s
- Allowed lateness: 30s; late events → DLQ side output (not silently dropped)
- Checkpoint interval: 60s, exactly-once
- Restart strategy: exponential delay, max 5 attempts, cap 5min
- State TTL: set explicitly on every keyed state; never leave unbounded

## L2 order book job (stateful)

Each `(exchange, symbol)` pair maintains:
- current bid/ask levels as sorted dict
- `last_update_id` for gap detection

On `last_update_id` gap: emit re-snapshot request event, reset state for that key.

Derived outputs per tick:
- best bid price + qty
- best ask price + qty
- bid-ask spread
- order book imbalance (bid_vol_top5 / (bid_vol_top5 + ask_vol_top5))
- mid-price

## Iceberg sink requirements

- Exactly-once via Flink checkpointing + Iceberg two-phase commit
- Partition spec must match table design (no per-symbol partitions)
- Preserve `kafka_topic`, `kafka_partition`, `kafka_offset` in every output row

## OHLCV window job

- Tumbling window: 1 minute, event time
- Key by: `(exchange, symbol)`
- Late data: corrected by nightly Spark backfill; do not hold windows open indefinitely
- Output: open, high, low, close, volume, trade_count, vwap, first_ts, last_ts

## SQL organization — always use the file pattern

**Never write SQL strings inline in `main()`.** Put all DDL/DML in `.sql` files and load them with `execute_sql_file()` / `add_inserts_from_file()` from `lib/sql_runner.py`.

```
jobs/
  sql/
    catalogs.sql          # CREATE CATALOG / CREATE DATABASE
    <job>/
      source.sql          # Kafka / Iceberg source table DDL
      sink_iceberg.sql    # Iceberg output table DDL
      sink_kafka.sql      # Kafka output table DDL (if needed)
      insert.sql          # INSERT INTO … SELECT … statements only
  lib/
    sql_runner.py         # execute_sql_file(), add_inserts_from_file()
    config.py             # has .as_dict() for template substitution
```

In Python, call:
```python
execute_sql_file(t_env, SQL / "catalogs.sql", **cfg.as_dict())
execute_sql_file(t_env, SQL / "normalize/source.sql", **cfg.as_dict())

stmt_set = t_env.create_statement_set()
add_inserts_from_file(stmt_set, SQL / "normalize/insert.sql")
stmt_set.execute()  # never .wait() — blocks the driver forever for streaming jobs
```

In `.sql` files, use `{param_name}` placeholders for runtime values:
```sql
'properties.bootstrap.servers' = '{kafka_brokers}'
```

`config.py` must expose `as_dict() -> dict[str, str]` so all jobs share a single substitution call.

**INSERT files**: one `INSERT INTO … SELECT …` per statement, separated by `;`.
**DDL files**: multiple `CREATE TABLE` / `CREATE CATALOG` statements, separated by `;`.
`sql_runner.py` splits on `;` and filters blank/comment-only blocks — no EXECUTE STATEMENT SET inside `.sql` files (use the Python statement-set API instead).

## PyFlink 1.18 operational pitfalls

These are confirmed bugs/limitations in the Python bindings — not config errors.

**`stmt_set.execute().wait()` blocks forever for streaming jobs.**  
`execute()` submits the job and returns immediately. `.wait()` then blocks the Python driver process waiting for the (infinite) job to finish. The job never actually runs. Remove `.wait()`.

**`ctx.output()` side outputs are not supported in Python.**  
`InternalKeyedProcessFunctionContext` has no `output()` method. Route late/DLQ events via `log.warning()` and return instead.

**`process_element` uses `yield`, not `out.collect()`.**  
The Python API has no `out` collector parameter. Use `yield Row(...)` to emit results.

**Timestamps: use epoch-ms (BIGINT), not datetime/Instant.**  
Python `datetime` ↔ Java `Instant` conversion in PyFlink 1.18 is ambiguous. Emit timestamps as `int` milliseconds; convert to `TIMESTAMP_LTZ` in the Table schema using `TO_TIMESTAMP_LTZ(col_ms, 3)`.

**structlog is incompatible with the PyFlink Beam runner.**  
PyFlink replaces Python's `print()` with `CustomPrint`, which doesn't accept the `flush=True` kwarg. structlog calls `print(msg, flush=True)` internally → `TypeError`. Use standard `logging` in all job code; do not install structlog in the flink Docker image.

**Rebuild ALL THREE flink images together after any change.**  
`flink run` (flink-init) uses cloudpickle to serialize `KeyedProcessFunction` subclasses into the job graph at submission time. The pickled bytes include references to every Python package imported in the function. If flink-init and flink-taskmanager have different packages installed, deserialization on the taskmanager fails with `ModuleNotFoundError`. Always rebuild together:
```bash
docker compose build --no-cache flink-jobmanager flink-taskmanager flink-init
```

**`docker compose restart` does not use new images.**  
It only stops and starts the existing containers. After a rebuild, recreate:
```bash
docker compose up -d --force-recreate flink-jobmanager flink-taskmanager
```

**`AWS_REGION` env var is required for the Iceberg AWS S3FileIO.**  
AWS SDK v2's `DefaultAwsRegionProviderChain` ignores Flink config and Iceberg catalog properties. It only reads the `AWS_REGION` environment variable (or EC2 metadata). Set it in docker-compose.yml for all three flink services (jobmanager, taskmanager, flink-init). The catalog `'s3.region'` property alone is not enough.

**Docker Desktop disk fills up after multiple `--no-cache` rebuilds.**  
Each rebuild without cache stores fresh layers. The jobmanager BLOB server writes to `/tmp` on the container filesystem; if the Docker VM disk is full, job submission fails with `Connection reset by peer`. Run `docker system prune -f` to reclaim space.

**Iceberg sinks write only on checkpoint boundaries (~60s).**  
`SELECT count(*) FROM iceberg.normalized.book_ticker` returns 0 until the first checkpoint completes. Confirm checkpoints are happening:
```bash
docker compose logs --tail=20 flink-taskmanager | grep "checkpointId"
```

## Anti-patterns

- Writing SQL strings inline in `main()` — use `.sql` files instead
- Using processing time when event time is available
- Keyed state without TTL (state grows unbounded)
- Side effects (I/O, network calls) inside map/filter operators
- Skipping watermark configuration ("it still processes" — until late events corrupt windows)
- `restart-strategy: none` in any environment
- Iceberg sink without checkpointing (data loss on failure)
- `.wait()` on `stmt_set.execute()` — blocks driver forever for streaming jobs
- Installing structlog in the flink Docker image
- Rebuilding only some of the three flink images (jobmanager/taskmanager/flink-init)
