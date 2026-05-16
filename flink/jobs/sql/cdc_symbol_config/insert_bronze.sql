-- cdc_symbol_config/insert_bronze.sql: Append all CDC events to raw.symbol_config_cdc.
--
-- bronze_view is a DataStream-backed Table view created in cdc_symbol_config.py.
-- All op codes (r/c/u/d) are written; no filtering — the bronze layer is the
-- immutable audit log.

INSERT INTO iceberg_cat.bronze.symbol_config_cdc
SELECT
    op,
    symbol,
    before_json,
    after_json,
    source_lsn,
    source_ts_ms,
    source_tx_id,
    debezium_ts_ms,
    kafka_topic,
    kafka_partition,
    kafka_offset,
    ingest_ts
FROM bronze_view
