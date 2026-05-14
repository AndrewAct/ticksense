---
name: spark-airflow-backfill
description: Use when implementing Airflow DAGs, Spark backfill jobs, Iceberg compaction, or Great Expectations checks.
---

# Spark + Airflow Backfill

## Airflow DAG requirements

Every DAG must be:
- **Idempotent**: running the same DAG run twice produces identical results
- **Parameterized**: accept `start_date` and `end_date` as params (not hardcoded)
- **Retryable**: `retries=3`, exponential backoff, `max_retry_delay=30min`
- **Catchup-safe**: `catchup=True` only when each run is genuinely independent

```python
default_args = {
    "retries": 3,
    "retry_delay": timedelta(minutes=2),
    "retry_exponential_backoff": True,
    "max_retry_delay": timedelta(minutes=30),
}
```

## TickSense DAGs

```
backfill_ohlcv_1m           Spark: recompute OHLCV from raw for a date range
compact_iceberg_tables       Spark: rewrite_data_files on streaming-written tables
expire_iceberg_snapshots     keep last 7 days of snapshots
run_dbt_models               dbt run + dbt test
run_great_expectations       validate freshness + row counts + nulls
freshness_sla_check          alert if max(exchange_event_ts) > now - 35s for any symbol
```

## Spark write pattern (idempotent)

Always dynamic partition overwrite; never blind append on backfill:

```python
spark.conf.set("spark.sql.sources.partitionOverwriteMode", "dynamic")
df.write.format("iceberg").mode("overwrite").saveAsTable("normalized.ohlcv_1m")
```

## Great Expectations checks (minimum per table)

- `expect_column_values_to_not_be_null` on `exchange`, `symbol`, `exchange_event_ts`
- `expect_column_values_to_be_between` on price (> 0) and volume (≥ 0)
- `expect_table_row_count_to_be_between` per `(exchange, symbol, date)` partition
- Freshness: `max(exchange_event_ts) > now() - interval '35 seconds'` per active symbol

## Backfill task ordering

```
start
  └─ validate_source_availability
       └─ spark_backfill_raw (idempotent overwrite)
            └─ spark_compute_ohlcv (idempotent overwrite)
                 └─ dbt_run_marts
                      └─ great_expectations_check
                           └─ freshness_sla_check
                                └─ end
```

## Anti-patterns

- Airflow tasks with non-idempotent side effects (blind appends without partition isolation)
- Spark jobs without explicit partition spec (full table scan + rewrite)
- `catchup=True` on DAGs that are not idempotent
- Swallowing task exceptions to avoid retries
- Hard-coded date ranges in task definitions
