-- normalize/insert.sql: Fan-out to Iceberg silver table + Kafka intermediate topic.
--
-- Both INSERT statements read book_ticker_view once (shared source scan).
-- Added to a Python StatementSet so they run as a single Flink job.
-- Column aliases map src_kafka_* (DataStream field names) to kafka_* (sink names).

INSERT INTO iceberg_cat.normalized.book_ticker
SELECT
    event_id,
    exchange,
    symbol,
    exchange_event_ts,
    ingest_ts,
    processed_ts,
    last_update_id,
    first_update_id,
    best_bid_price,
    best_bid_qty,
    best_ask_price,
    best_ask_qty,
    spread,
    mid_price,
    imbalance,
    src_kafka_topic     AS kafka_topic,
    src_kafka_partition AS kafka_partition,
    src_kafka_offset    AS kafka_offset
FROM book_ticker_view;

INSERT INTO book_ticker_kafka_sink
SELECT
    event_id,
    exchange,
    symbol,
    exchange_event_ts,
    ingest_ts,
    processed_ts,
    last_update_id,
    first_update_id,
    best_bid_price,
    best_bid_qty,
    best_ask_price,
    best_ask_qty,
    spread,
    mid_price,
    imbalance,
    src_kafka_topic     AS kafka_topic,
    src_kafka_partition AS kafka_partition,
    src_kafka_offset    AS kafka_offset
FROM book_ticker_view
