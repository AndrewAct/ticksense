-- cdc_symbol_config/sink_normalized.sql: Normalized symbol config (upsert, Iceberg v2).
--
-- Maintains the CURRENT STATE of each trading pair.  Flink's debezium-json
-- source emits RowKind-annotated rows; the Iceberg v2 sink translates them:
--   RowKind.INSERT / UPDATE_AFTER → upsert by symbol (insert or overwrite)
--   RowKind.DELETE                → equality-delete on symbol
--
-- Requires Iceberg format-version=2 for equality-delete file support.
-- 'write.upsert.enabled' triggers the equality-delete upsert path in the
-- Iceberg Flink sink; identifier field defaults to the PRIMARY KEY (symbol).
--
-- Timestamps arrive from Debezium as epoch-ms BIGINT (time.precision.mode=connect);
-- conversion to TIMESTAMP_LTZ happens in insert_normalized.sql.

CREATE TABLE IF NOT EXISTS iceberg_cat.normalized.symbol_config (
    symbol          STRING,
    exchange        STRING,
    status          STRING,
    base_asset      STRING,
    quote_asset     STRING,
    lot_size_min    DOUBLE,
    lot_size_step   DOUBLE,
    tick_size       DOUBLE,
    created_at      TIMESTAMP_LTZ(3),
    updated_at      TIMESTAMP_LTZ(3),
    PRIMARY KEY (symbol) NOT ENFORCED
) WITH (
    'format-version'       = '2',
    'write.upsert.enabled' = 'true'
)
