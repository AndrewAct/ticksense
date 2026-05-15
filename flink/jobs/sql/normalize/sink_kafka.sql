-- normalize/sink_kafka.sql: Kafka sink for normalized book-ticker events.
--
-- Consumed by the ohlcv_1m job.  Schema mirrors iceberg_cat.normalized.book_ticker
-- so ohlcv_1m/source.sql can use an identical column list.

CREATE TEMPORARY TABLE book_ticker_kafka_sink (
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
    kafka_offset        BIGINT
) WITH (
    'connector'                      = 'kafka',
    'topic'                          = '{topic_book_ticker}',
    'properties.bootstrap.servers'   = '{kafka_brokers}',
    'format'                         = 'json',
    'json.timestamp-format.standard' = 'ISO-8601'
)
