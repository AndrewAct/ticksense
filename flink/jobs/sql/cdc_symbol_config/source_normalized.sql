-- cdc_symbol_config/source_normalized.sql: Kafka source for the normalized upsert path.
--
-- Uses Flink's built-in debezium-json format (part of flink-dist, no extra JAR).
-- With PRIMARY KEY defined, Flink treats this as a changelog stream:
--   op=r/c → RowKind.INSERT
--   op=u   → RowKind.UPDATE_BEFORE + RowKind.UPDATE_AFTER
--   op=d   → RowKind.DELETE
--
-- Column types must match what Debezium emits in the after/before payload:
--   NUMERIC(20,8) → DOUBLE  (decimal.handling.mode=double)
--   TIMESTAMPTZ   → BIGINT  (epoch ms, time.precision.mode=connect)
--
-- Separate consumer group from the bronze DataStream source so both paths
-- make independent progress and can be reset independently.

CREATE TEMPORARY TABLE cdc_symbol_config_source (
    symbol          STRING,
    exchange        STRING,
    status          STRING,
    base_asset      STRING,
    quote_asset     STRING,
    lot_size_min    DOUBLE,
    lot_size_step   DOUBLE,
    tick_size       DOUBLE,
    created_at      BIGINT,     -- epoch ms; converted to TIMESTAMP_LTZ in insert_normalized.sql
    updated_at      BIGINT,
    PRIMARY KEY (symbol) NOT ENFORCED
) WITH (
    'connector'                         = 'kafka',
    'topic'                             = '{topic_cdc}',
    'properties.bootstrap.servers'      = '{kafka_brokers}',
    'properties.group.id'               = '{group_cdc_normalized}',
    'scan.startup.mode'                 = 'earliest-offset',
    'format'                            = 'debezium-json',
    'debezium-json.schema-include'      = 'false',
    'debezium-json.ignore-parse-errors' = 'true'
)
