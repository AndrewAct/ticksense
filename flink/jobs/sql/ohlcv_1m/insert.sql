-- ohlcv_1m/insert.sql: 1-minute tumbling-window OHLCV aggregation.
--
-- Keyed by (exchange, symbol), event-time windows.
-- VWAP uses best_bid_qty as the volume weight (L2 book proxy).
-- COALESCE(best_bid_qty, 0.0) guards against null qtys from parse errors.

INSERT INTO iceberg_cat.normalized.ohlcv_1m
SELECT
    exchange,
    symbol,
    window_start,
    window_end,
    FIRST_VALUE(mid_price)  AS open_price,
    MAX(mid_price)          AS high_price,
    MIN(mid_price)          AS low_price,
    LAST_VALUE(mid_price)   AS close_price,
    SUM(COALESCE(best_bid_qty, 0.0)) AS volume,
    CASE
        WHEN SUM(COALESCE(best_bid_qty, 0.0)) > 0
        THEN SUM(mid_price * COALESCE(best_bid_qty, 0.0))
             / SUM(COALESCE(best_bid_qty, 0.0))
        ELSE AVG(mid_price)
    END                     AS vwap,
    COUNT(*)                AS tick_count,
    MIN(exchange_event_ts)  AS first_ts,
    MAX(exchange_event_ts)  AS last_ts
FROM TABLE(
    TUMBLE(
        TABLE normalized_book_ticker,
        DESCRIPTOR(exchange_event_ts),
        INTERVAL '1' MINUTE
    )
)
WHERE mid_price IS NOT NULL
GROUP BY exchange, symbol, window_start, window_end
