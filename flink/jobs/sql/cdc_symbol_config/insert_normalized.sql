-- cdc_symbol_config/insert_normalized.sql: Upsert current symbol state from CDC stream.
--
-- cdc_symbol_config_source produces a changelog stream (RowKind-annotated) because
-- it has PRIMARY KEY and uses debezium-json format.  The Iceberg v2 sink with
-- write.upsert.enabled=true translates RowKinds to equality-delete + append:
--   INSERT / UPDATE_AFTER → upsert (delete old row by symbol, insert new)
--   DELETE                → equality-delete by symbol
--
-- TO_TIMESTAMP_LTZ(col, 3) converts epoch-ms BIGINT → TIMESTAMP_LTZ(3).

INSERT INTO iceberg_cat.normalized.symbol_config
SELECT
    symbol,
    exchange,
    status,
    base_asset,
    quote_asset,
    lot_size_min,
    lot_size_step,
    tick_size,
    TO_TIMESTAMP_LTZ(created_at, 3) AS created_at,
    TO_TIMESTAMP_LTZ(updated_at, 3) AS updated_at
FROM cdc_symbol_config_source
