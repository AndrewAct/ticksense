---
name: cdc-debezium-patterns
description: Use when implementing Postgres CDC with Debezium, processing CDC Kafka topics, or writing Iceberg upserts from CDC streams.
---

# CDC Debezium Patterns

## What CDC is used for in TickSense

CDC is for slowly-changing reference and configuration data, not market ticks:
- `symbol_config` — trading pairs, lot sizes, tick sizes, status
- `exchange_config` — exchange metadata and rate limits
- `alert_rule` — user-defined price/spread alert thresholds

Do not route market tick data through Postgres → CDC. Use the Kafka ingestion path directly.

## Debezium op codes

| op | meaning | required action |
|---|---|---|
| `r` | snapshot read | upsert (treat as create) |
| `c` | create | insert / upsert by primary key |
| `u` | update | upsert by primary key |
| `d` | delete | soft delete or tombstone handling |

Always check `op` before processing. Never assume all messages are inserts.

## Required fields to preserve

```python
class CdcEvent(BaseModel):
    source_table: str
    primary_key: str           # stringified PK value
    op: Literal["r", "c", "u", "d"]
    source_lsn: int            # Postgres LSN; use for ordering on replay
    source_ts: datetime        # commit timestamp in Postgres
    kafka_topic: str
    kafka_partition: int
    kafka_offset: int
    before: dict | None        # null for inserts and snapshot reads
    after: dict | None         # null for deletes
```

## Iceberg upsert pattern

```sql
MERGE INTO normalized.symbol_config t
USING staged_updates s ON t.primary_key = s.primary_key
WHEN MATCHED AND s.op = 'd' THEN DELETE
WHEN MATCHED THEN UPDATE SET
    symbol = s.after['symbol'],
    updated_at = s.source_ts,
    source_lsn = s.source_lsn
WHEN NOT MATCHED AND s.op != 'd' THEN INSERT (...)
```

Always upsert by primary key. Never append CDC events as new rows to a normalized table.

## Replay protocol

1. Identify target Kafka offset range: `(topic, partition, start_offset, end_offset)`
2. Roll back Iceberg table to pre-write snapshot if needed
3. Seek consumer to `start_offset`
4. Reprocess events in `source_lsn` order; upsert is idempotent
5. Verify row counts and spot-check values post-replay

## Tombstone messages

Debezium sends a tombstone (null value, original key) after a hard delete.
Configure consumer to handle null values without crashing:
```python
if msg.value() is None:
    handle_tombstone(msg.key())
    return
```

## Anti-patterns

- Ignoring `op` field (all events treated as inserts)
- Processing CDC without checking `source_lsn` order on replay
- Routing high-volume market ticks through Postgres CDC
- Missing tombstone handling → consumer crash on hard delete
- Using `before` field without null-checking (null for `c` and `r` events)
