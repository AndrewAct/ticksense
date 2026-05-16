-- cdc_symbol_config/sink_bronze.sql: Bronze CDC event store (append-only).
--
-- Stores every Debezium event verbatim — op code, full before/after payloads,
-- Postgres WAL metadata, and Kafka provenance.  Never modified after write.
-- This is the canonical replay source: to re-derive normalized.symbol_config,
-- consume this table in source_lsn order.
--
-- Partitioned by symbol so Trino can prune to a single pair efficiently.
-- format-version=1 (append-only; no equality deletes needed for bronze).

CREATE TABLE IF NOT EXISTS iceberg_cat.bronze.symbol_config_cdc (
    op              STRING,     -- Debezium op code: r=snapshot, c=create, u=update, d=delete
    symbol          STRING,     -- extracted from after (or before on d) for partitioning
    before_json     STRING,     -- Debezium 'before' field as JSON string; NULL for r and c ops
    after_json      STRING,     -- Debezium 'after' field as JSON string; NULL for d ops
    source_lsn      BIGINT,     -- Postgres WAL byte position; use for replay ordering
    source_ts_ms    BIGINT,     -- Postgres commit timestamp (epoch ms, connect mode)
    source_tx_id    BIGINT,     -- Postgres transaction ID
    debezium_ts_ms  BIGINT,     -- Debezium processing timestamp (epoch ms)
    kafka_topic     STRING,
    kafka_partition INT,
    kafka_offset    BIGINT,
    ingest_ts       TIMESTAMP_LTZ(3)
) PARTITIONED BY (symbol)
WITH (
    'write.format.default'             = 'parquet',
    'write.metadata.compression-codec' = 'gzip'
)
