---
name: dbt-analytics-engineering
description: Use when implementing or modifying dbt models, schema tests, source freshness, or model documentation.
---

# dbt Analytics Engineering

## Model layers

```
models/
  staging/         1:1 with source Iceberg tables; light casting and renaming only
  intermediate/    Business logic; not exposed to downstream consumers directly
  marts/           Final aggregates; safe for API and dashboards
```

## TickSense models

```
staging:
  stg_orderbook_diffs          from raw.orderbook_diffs
  stg_book_ticker              from normalized.book_ticker
  stg_symbol_config            from normalized.symbol_config

intermediate:
  int_ohlcv_1m                 windowed OHLCV from stg_book_ticker
  int_spread_metrics           bid-ask spread, mid-price per symbol per minute
  int_order_book_imbalance     bid_vol / (bid_vol + ask_vol) top-5 levels
  int_liquidity_score          composite score per symbol
  int_freshness_status         is each symbol receiving data within SLA?

marts:
  mart_ohlcv                   gap-filled OHLCV with backfill flag
  mart_liquidity               liquidity metrics for FastAPI
  mart_volatility              realized vol (5m, 1h, 1d rolling)
  mart_exchange_health         freshness + error rate per exchange
```

## Materialization strategy

- `staging`: `view` (no storage cost, always fresh)
- `intermediate`: `ephemeral` (inlined into downstream queries)
- `marts`: `table` (pre-computed, fast for API)

## Required schema.yml for every model

```yaml
models:
  - name: mart_ohlcv
    description: "Gap-filled 1-minute OHLCV for all active symbols."
    columns:
      - name: symbol
        description: "Trading pair, e.g. BTCUSDT"
        tests: [not_null]
      - name: window_start
        tests: [not_null]
      - name: open
        tests: [not_null]
```

Add `unique` test on grain columns where applicable (e.g. `(exchange, symbol, window_start)`).

## Source freshness (sources.yml)

```yaml
sources:
  - name: normalized
    freshness:
      warn_after: {count: 35, period: second}
      error_after: {count: 60, period: second}
    loaded_at_field: ingest_ts
```

## Anti-patterns

- Business logic in staging models
- `SELECT *` in any model
- Marts that join more than 3 tables without an intermediate layer
- Missing `not_null` tests on grain columns
- Models without a `description` in schema.yml
