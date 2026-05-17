-- backfill_ohlcv.sql
-- Recompute 1-minute OHLCV from normalized.book_ticker for a date range.
--
-- Parameters (substituted by Python .format()):
--   {start_ts}  — inclusive lower bound, ISO timestamp string
--   {end_ts}    — exclusive upper bound, ISO timestamp string
--
-- open/close correctness: ROW_NUMBER picks the first and last tick by time
-- within each minute window, avoiding the undefined ordering of first()/last().

WITH ranked AS (
    SELECT
        exchange,
        symbol,
        mid_price,
        DATE_TRUNC('MINUTE', exchange_event_ts)   AS window_start,
        ROW_NUMBER() OVER (
            PARTITION BY exchange, symbol,
                         DATE_TRUNC('MINUTE', exchange_event_ts)
            ORDER BY exchange_event_ts ASC
        )                                         AS rn_asc,
        ROW_NUMBER() OVER (
            PARTITION BY exchange, symbol,
                         DATE_TRUNC('MINUTE', exchange_event_ts)
            ORDER BY exchange_event_ts DESC
        )                                         AS rn_desc
    FROM iceberg.normalized.book_ticker
    WHERE exchange_event_ts >= TIMESTAMP '{start_ts}'
      AND exchange_event_ts <  TIMESTAMP '{end_ts}'
)
SELECT
    exchange,
    symbol,
    window_start,
    window_start + INTERVAL 1 MINUTES                AS window_end,
    MAX(CASE WHEN rn_asc  = 1 THEN mid_price END)    AS open,
    MAX(mid_price)                                    AS high,
    MIN(mid_price)                                    AS low,
    MAX(CASE WHEN rn_desc = 1 THEN mid_price END)    AS close,
    AVG(mid_price)                                    AS vwap,
    COUNT(*)                                          AS trade_count,
    CAST(window_start AS DATE)                        AS event_date,
    CURRENT_TIMESTAMP()                               AS ingest_ts
FROM ranked
GROUP BY exchange, symbol, window_start
ORDER BY exchange, symbol, window_start
