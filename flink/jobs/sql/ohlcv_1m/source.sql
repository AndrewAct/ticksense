-- ohlcv_1m/source.sql: Kafka source for normalized book-ticker events.
--
-- Produced by the normalize job into market.normalized.book_ticker.
-- Schema is identical to normalize/sink_kafka.sql.
-- Watermark: 5-second bounded out-of-orderness on exchange_event_ts.

CREATE TEMPORARY TABLE normalized_book_ticker (
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
    WATERMARK FOR exchange_event_ts AS exchange_event_ts - INTERVAL '5' SECOND
) WITH (
    'connector'                      = 'kafka',
    'topic'                          = '{topic_book_ticker}',
    'properties.bootstrap.servers'   = '{kafka_brokers}',
    'properties.group.id'            = '{group_ohlcv}',
    'scan.startup.mode'              = 'earliest-offset',
    'format'                         = 'json',
    'json.timestamp-format.standard' = 'ISO-8601',
    'json.fail-on-missing-field'     = 'false',
    'json.ignore-parse-errors'       = 'true'
)
