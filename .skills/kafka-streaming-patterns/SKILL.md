---
name: kafka-streaming-patterns
description: Use when implementing Kafka producers, consumers, topic design, schemas, DLQ, or replay logic.
---

# Kafka Streaming Patterns

## Before writing code, answer

1. What is the topic name? (follows `market.*` or `postgres.*` convention)
2. What is the message key? (`exchange#symbol` or `table#primary_key`)
3. What is the dedup key?
4. What happens to malformed messages? (→ DLQ, never silently dropped)
5. What are the producer retry settings?
6. What is the consumer group name and offset commit strategy?

## TickSense topic conventions

```
market.raw.orderbook        key: binance#btcusdt   dedup: binance#btcusdt#last_update_id
market.normalized.trades    key: binance#btcusdt   dedup: binance#btcusdt#trade_id
market.agg.ohlcv_1m         key: binance#btcusdt
market.signal.events        key: binance#btcusdt
postgres.public.<table>     key: <table>#<pk>      (Debezium-managed)
market.dlq                  key: original message key
```

## Required fields in every market event

```python
class MarketEvent(BaseModel):
    event_id: str            # deterministic dedup key
    exchange: str
    symbol: str
    exchange_event_ts: datetime
    ingest_ts: datetime
    # populated post-produce / post-consume:
    kafka_topic: str | None = None
    kafka_partition: int | None = None
    kafka_offset: int | None = None
```

## Producer requirements

- `enable.idempotence=true`
- `acks=all`
- `retries` ≥ 3 with exponential backoff
- Key: always set, never null
- Value: schema-validated before sending
- On serialization error: log + send to DLQ

## Consumer requirements

- Commit offsets only after successful downstream processing
- On error: retry ≤ 3 times, then route to DLQ with error metadata appended
- `auto.offset.reset=earliest` in all environments

## L2 order book specifics

Binance sends depth diffs. The consumer must:
1. Fetch REST snapshot: `GET /api/v3/depth?symbol=BTCUSDT&limit=1000`
2. Buffer WebSocket diffs during snapshot fetch
3. Drop diffs where `lastUpdateId` ≤ snapshot `lastUpdateId`
4. Apply remaining diffs in order; detect gaps by checking `U` ≤ `lastUpdateId + 1`
5. On gap: restart from step 1

## Anti-patterns

- `producer.send(topic, json.dumps(event))` — no key, no validation, no error handling
- `auto.offset.reset=latest` — silently skips events on consumer restart
- Catching and swallowing `KafkaException`
- Partitioning by timestamp — use `exchange#symbol` for co-locality
- Producing without flushing before shutdown
