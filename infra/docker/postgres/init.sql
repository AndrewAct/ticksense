-- init.sql: Bootstrap schema for TickSense Postgres database.
--
-- Executed once by the postgres-init service (psql -f init.sql).
-- WAL level is already set to logical in docker-compose.yml command flags.
-- Debezium uses the built-in pgoutput plugin (available since Postgres 10).
--
-- ─── Why this table exists ───────────────────────────────────────────────────
-- symbol_config stores slowly-changing reference data: trading-pair metadata
-- (lot sizes, tick sizes, status).  Changes here flow via Debezium CDC into
-- Kafka → Flink → Iceberg normalized.symbol_config.  Market tick data is NOT
-- routed through Postgres — that path uses the direct Kafka ingest pipeline.

CREATE TABLE IF NOT EXISTS symbol_config (
    symbol          VARCHAR(20)    PRIMARY KEY,
    exchange        VARCHAR(20)    NOT NULL DEFAULT 'binance',
    status          VARCHAR(20)    NOT NULL DEFAULT 'TRADING',
    base_asset      VARCHAR(20)    NOT NULL,
    quote_asset     VARCHAR(20)    NOT NULL,
    -- Lot size: minimum order quantity and step increment (NUMERIC for precision)
    lot_size_min    NUMERIC(20,8)  NOT NULL DEFAULT 0,
    lot_size_step   NUMERIC(20,8)  NOT NULL DEFAULT 0,
    -- Tick size: minimum price movement
    tick_size       NUMERIC(20,8)  NOT NULL DEFAULT 0,
    created_at      TIMESTAMPTZ    NOT NULL DEFAULT NOW(),
    updated_at      TIMESTAMPTZ    NOT NULL DEFAULT NOW()
);

-- REPLICA IDENTITY FULL: Debezium needs complete old-row image in WAL so that
-- the 'before' field is populated for UPDATE/DELETE events.  Without this,
-- 'before' is null on UPDATE and Flink debezium-json drops the record.
ALTER TABLE symbol_config REPLICA IDENTITY FULL;

-- Seed the 5 symbols that the ingest service subscribes to.
-- ON CONFLICT DO NOTHING makes this script idempotent on re-run.
INSERT INTO symbol_config
    (symbol, exchange, status, base_asset, quote_asset, lot_size_min, lot_size_step, tick_size)
VALUES
    ('BTCUSDT', 'binance', 'TRADING', 'BTC', 'USDT', 0.00001000, 0.00001000, 0.01000000),
    ('ETHUSDT', 'binance', 'TRADING', 'ETH', 'USDT', 0.00010000, 0.00010000, 0.01000000),
    ('SOLUSDT', 'binance', 'TRADING', 'SOL', 'USDT', 0.00100000, 0.00100000, 0.00100000),
    ('BNBUSDT', 'binance', 'TRADING', 'BNB', 'USDT', 0.00100000, 0.00100000, 0.01000000),
    ('XRPUSDT', 'binance', 'TRADING', 'XRP', 'USDT', 0.10000000, 0.10000000, 0.00010000)
ON CONFLICT (symbol) DO NOTHING;

-- Verify table and row count — visible in postgres-init container logs.
DO $$
DECLARE row_count INT;
BEGIN
    SELECT COUNT(*) INTO row_count FROM symbol_config;
    RAISE NOTICE 'symbol_config initialized: % rows', row_count;
END $$;
