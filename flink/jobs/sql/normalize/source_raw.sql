-- normalize/source_raw.sql: Minimal Kafka source for DataStream consumption.
--
-- Uses 'format'='raw' so each message value arrives as a single STRING (the raw
-- JSON payload).  No ARRAY/MAP columns means the Table->DataStream bridge via
-- to_append_stream() works without type-mapping issues.
--
-- Metadata columns supply the real Kafka partition and offset so Iceberg rows
-- preserve full lineage for offset-based replay.

CREATE TEMPORARY TABLE raw_orderbook_stream (
    payload         STRING,
    kafka_partition INT     METADATA FROM 'partition' VIRTUAL,
    kafka_offset    BIGINT  METADATA FROM 'offset'    VIRTUAL
) WITH (
    'connector'                    = 'kafka',
    'topic'                        = '{topic_raw}',
    'properties.bootstrap.servers' = '{kafka_brokers}',
    'properties.group.id'          = '{group_normalize}',
    'scan.startup.mode'            = 'earliest-offset',
    'format'                       = 'raw'
)
