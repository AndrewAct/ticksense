-- normalize/sink_iceberg.sql: Silver book-ticker Iceberg table.
--
-- Stores one row per (exchange, symbol, last_update_id) with derived metrics.
-- kafka_topic/partition/offset preserved for Iceberg time-travel replay.
-- Partitioned by (exchange, symbol) — no date partition to keep layout simple
-- at this scale; add DAY(exchange_event_ts) when row counts justify it.

CREATE TABLE IF NOT EXISTS iceberg_cat.normalized.book_ticker (
    event_id            STRING,
    exchange            STRING,
    symbol              STRING,
    exchange_event_ts   TIMESTAMP_LTZ(3),
    ingest_ts           TIMESTAMP_LTZ(3),
    processed_ts        TIMESTAMP_LTZ(3),
    last_update_id      BIGINT,
    first_update_id     BIGINT,
    best_bid_price      DOUBLE,
    best_bid_qty        DOUBLE,
    best_ask_price      DOUBLE,
    best_ask_qty        DOUBLE,
    spread              DOUBLE,
    mid_price           DOUBLE,
    imbalance           DOUBLE,
    kafka_topic         STRING,
    kafka_partition     INT,
    kafka_offset        BIGINT,
    PRIMARY KEY (exchange, symbol, last_update_id) NOT ENFORCED
) PARTITIONED BY (exchange, symbol)
WITH (
    'write.format.default'              = 'parquet',
    'write.metadata.compression-codec'  = 'gzip'
)
