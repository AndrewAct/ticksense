-- ohlcv_1m/sink_iceberg.sql: Silver OHLCV Iceberg table, 1-minute granularity.
--
-- volume uses best_bid_qty as a proxy (L2 book ticks, not actual trades).
-- open/close are FIRST_VALUE/LAST_VALUE within the tumbling window — non-deterministic
-- by processing order, acceptable at this scale; replace with a Python UDAF for
-- strict event-time open/close if needed.

CREATE TABLE IF NOT EXISTS iceberg_cat.normalized.ohlcv_1m (
    exchange        STRING,
    symbol          STRING,
    window_start    TIMESTAMP_LTZ(3),
    window_end      TIMESTAMP_LTZ(3),
    open_price      DOUBLE,
    high_price      DOUBLE,
    low_price       DOUBLE,
    close_price     DOUBLE,
    volume          DOUBLE,
    vwap            DOUBLE,
    tick_count      BIGINT,
    first_ts        TIMESTAMP_LTZ(3),
    last_ts         TIMESTAMP_LTZ(3),
    PRIMARY KEY (exchange, symbol, window_start) NOT ENFORCED
) PARTITIONED BY (exchange, symbol)
WITH (
    'write.format.default'              = 'parquet',
    'write.metadata.compression-codec'  = 'gzip'
)
