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

## Anti-patterns

- Using processing time when event time is available
- Keyed state without TTL (state grows unbounded)
- Side effects (I/O, network calls) inside map/filter operators
- Skipping watermark configuration ("it still processes" — until late events corrupt windows)
- `restart-strategy: none` in any environment
- Iceberg sink without checkpointing (data loss on failure)
