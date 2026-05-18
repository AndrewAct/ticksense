-- cdc_symbol_config/source_bronze.sql: Raw-format Kafka source for the bronze DataStream path.
--
-- Each message value is read as a raw STRING (full Debezium JSON envelope preserved).
-- Separate consumer group from source_normalized.sql so both paths progress independently
-- and can be reset independently.
--
-- Metadata columns supply real partition and offset for Iceberg lineage.

CREATE TEMPORARY TABLE cdc_bronze_stream (
    payload         STRING,
    kafka_partition INT     METADATA FROM 'partition' VIRTUAL,
    kafka_offset    BIGINT  METADATA FROM 'offset'    VIRTUAL
) WITH (
    'connector'                    = 'kafka',
    'topic'                        = '{topic_cdc}',
    'properties.bootstrap.servers' = '{kafka_brokers}',
    'properties.group.id'          = '{group_cdc_bronze}',
    'scan.startup.mode'            = 'earliest-offset',
    'format'                       = 'raw'
)
