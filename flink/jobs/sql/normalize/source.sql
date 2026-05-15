-- normalize/source.sql: Kafka source for raw L2 order-book diff events.
--
-- bids / asks arrive as JSON [["price","qty"], ...] → ARRAY<ARRAY<STRING>>.
-- Metadata columns (kafka_topic/partition/offset) are read-only VIRTUAL.
-- Watermark: 5-second bounded out-of-orderness on exchange_event_ts.

CREATE TEMPORARY TABLE raw_orderbook (
    event_id            STRING,
    exchange            STRING,
    symbol              STRING,
    exchange_event_ts   TIMESTAMP_LTZ(3),
    ingest_ts           TIMESTAMP_LTZ(3),
    first_update_id     BIGINT,
    last_update_id      BIGINT,
    bids                ARRAY<ARRAY<STRING>>,
    asks                ARRAY<ARRAY<STRING>>,
    kafka_topic         STRING  METADATA FROM 'topic'     VIRTUAL,
    kafka_partition     INT     METADATA FROM 'partition' VIRTUAL,
    kafka_offset        BIGINT  METADATA FROM 'offset'    VIRTUAL,
    WATERMARK FOR exchange_event_ts AS exchange_event_ts - INTERVAL '5' SECOND
) WITH (
    'connector'                      = 'kafka',
    'topic'                          = '{topic_raw}',
    'properties.bootstrap.servers'   = '{kafka_brokers}',
    'properties.group.id'            = '{group_normalize}',
    'scan.startup.mode'              = 'earliest-offset',
    'format'                         = 'json',
    'json.timestamp-format.standard' = 'ISO-8601',
    'json.fail-on-missing-field'     = 'false',
    'json.ignore-parse-errors'       = 'true'
)
