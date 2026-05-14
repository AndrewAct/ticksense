---
name: lakehouse-iceberg-patterns
description: Use when designing Iceberg tables, partition specs, schema evolution, Trino queries, Spark writes, compaction, or snapshot rollback.
---

# Iceberg Lakehouse Patterns

## Table layers

| Layer | Schema prefix | Write mode | Purpose |
|---|---|---|---|
| Bronze | `raw.` | append-only | Raw payload + full Kafka metadata |
| Silver | `normalized.` | merge-on-read upsert | Typed, deduplicated, queryable |
| Gold | `marts.` | overwrite by partition | Aggregated, business-facing |

## TickSense tables

```
raw.orderbook_diffs           streaming L2 diff events
raw.orderbook_snapshots       on-demand REST snapshots (on gap / restart)
normalized.book_ticker        best bid/ask per symbol, deduplicated
normalized.ohlcv_1m           1m OHLCV, from Flink window job
normalized.liquidity_metrics  spread, imbalance, microprice per symbol
normalized.symbol_config      CDC from Postgres (upsert)
marts.ohlcv                   gap-filled, backfilled OHLCV
marts.liquidity               liquidity mart for API + dashboards
marts.volatility              realized vol from OHLCV
marts.exchange_health         freshness + error rates per exchange
```

## Required columns in all raw and normalized tables

```sql
exchange           VARCHAR NOT NULL
symbol             VARCHAR NOT NULL
exchange_event_ts  TIMESTAMP WITH TIME ZONE NOT NULL
ingest_ts          TIMESTAMP WITH TIME ZONE NOT NULL
kafka_topic        VARCHAR NOT NULL
kafka_partition    INT NOT NULL
kafka_offset       BIGINT NOT NULL
```

## Partitioning rules

Use:
```sql
PARTITIONED BY (days(exchange_event_ts), exchange)
```

Never:
```sql
PARTITIONED BY (symbol)        -- too many partitions, guaranteed small-file explosion
PARTITIONED BY (hours(...))    -- too granular for 5M events/day
PARTITIONED BY (kafka_offset)  -- meaningless for query patterns
```

## Small file management

Flink streaming writes create many small files. Run nightly Spark compaction:
```python
spark.sql("CALL iceberg.system.rewrite_data_files("
          "  table => 'normalized.book_ticker',"
          "  strategy => 'sort',"
          "  sort_order => 'exchange_event_ts ASC NULLS LAST')")
```

Also expire old snapshots to bound metadata size:
```python
spark.sql("CALL iceberg.system.expire_snapshots("
          "  table => 'normalized.book_ticker',"
          "  older_than => TIMESTAMP '2024-01-01 00:00:00',"
          "  retain_last => 7)")
```

## Schema evolution rules

- Adding nullable columns: safe
- Renaming: use Iceberg column rename API, not drop + add
- Dropping columns from raw tables: never
- Type widening: int → long, float → double are safe

## Replay via snapshot rollback

```python
# Roll back silver table to before a bad write
table.manage_snapshots().rollback_to_snapshot(snapshot_id).commit()
# Then re-seek Kafka consumer to the saved offset and reprocess
```

## Anti-patterns

- Partitioning by `symbol` (50 pairs × time granularity = thousands of tiny partitions)
- Running Trino queries directly on `raw.*` tables in user-facing APIs
- DROP TABLE + recreate instead of schema evolution (loses snapshot history)
- Forgetting `write.metadata.delete-after-commit.enabled=true`
