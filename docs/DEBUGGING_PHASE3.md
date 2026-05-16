# Phase 3 — CDC + Replay: Debug Runbook

Reference for common failures when setting up Debezium, the CDC Flink job, and the replay CLI.

---

## Architecture recap

```
Postgres (WAL logical replication)
    ↓  pgoutput plugin
Debezium Connect (port 8083)
    ↓  JSON, schemas.enable=false
Kafka topic: postgres.public.symbol_config
    ↓  consumer group flink-cdc-bronze       → Iceberg raw.symbol_config_cdc     (append)
    ↓  consumer group flink-cdc-normalized   → Iceberg normalized.symbol_config   (upsert)
```

Debezium envelope format (one message per committed DML):
```json
{
  "before": { "symbol": "BTCUSDT", ... } | null,
  "after":  { "symbol": "BTCUSDT", ... } | null,
  "source": { "lsn": 23760000, "ts_ms": 1747267200000, "txId": 742, ... },
  "op":     "r" | "c" | "u" | "d",
  "ts_ms":  1747267200000
}
```

op codes:
| op | when | before | after |
|----|------|--------|-------|
| r  | initial snapshot | null | row data |
| c  | INSERT | null | new row |
| u  | UPDATE | old row | new row |
| d  | DELETE | old row | null |

---

## Verification checklist (run after `make up`)

### 1. Debezium connector is registered and running

```bash
# List connectors
curl -sf http://localhost:8083/connectors | jq .

# Connector status — should be RUNNING
curl -sf http://localhost:8083/connectors/ticksense-symbol-config/status | jq .

# Expected output:
# { "connector": { "state": "RUNNING", ... },
#   "tasks": [{ "state": "RUNNING", ... }] }
```

### 2. Snapshot events are in Kafka

After startup, Debezium takes an initial snapshot: it reads all rows from
symbol_config and emits one `op=r` event per row.

```bash
# Should show 5 messages (one per seed symbol)
docker compose exec redpanda \
  rpk topic consume postgres.public.symbol_config --brokers localhost:9092 -n 5
```

Expected message structure:
```json
{"before":null,"after":{"symbol":"BTCUSDT","exchange":"binance",...},"op":"r","source":{"lsn":...}}
```

### 3. Trigger a CDC event manually

```bash
# Connect to Postgres
docker compose exec postgres psql -U ticksense -d ticksense

-- UPDATE triggers op=u
UPDATE symbol_config SET status = 'BREAK' WHERE symbol = 'BTCUSDT';

-- INSERT triggers op=c
INSERT INTO symbol_config (symbol, exchange, status, base_asset, quote_asset, lot_size_min, lot_size_step, tick_size)
VALUES ('DOGEUSDT', 'binance', 'TRADING', 'DOGE', 'USDT', 1.0, 1.0, 0.00001);

-- DELETE triggers op=d (tombstone suppressed via tombstones.on.delete=false)
DELETE FROM symbol_config WHERE symbol = 'DOGEUSDT';
```

Then check Kafka:
```bash
docker compose exec redpanda \
  rpk topic consume postgres.public.symbol_config --brokers localhost:9092 -n 20
```

### 4. Flink CDC job is running

```bash
# Job list — cdc_symbol_config should appear as RUNNING
curl -sf http://localhost:8081/jobs | jq '.jobs[] | select(.status=="RUNNING") | .name'
```

### 5. Iceberg bronze table has rows

```bash
docker compose exec trino trino --execute \
  "SELECT op, symbol, source_lsn, ingest_ts FROM iceberg.raw.symbol_config_cdc ORDER BY source_lsn LIMIT 20"
```

### 6. Iceberg normalized table has current state (upserted)

```bash
docker compose exec trino trino --execute \
  "SELECT symbol, status, lot_size_min FROM iceberg.normalized.symbol_config ORDER BY symbol"
```

Expected: 5 rows (one per symbol), current values only — not historical append.

---

## Common failures

### Debezium connector fails with "replication slot already exists"

```
ERROR: replication slot "ticksense_debezium" already exists
```

Cause: Previous run left a slot behind (e.g., `make down -v` didn't clean Postgres WAL state,
or the volume was not removed).

Fix:
```bash
docker compose exec postgres psql -U ticksense -d ticksense \
  -c "SELECT pg_drop_replication_slot('ticksense_debezium');"
# Then restart Debezium:
docker compose restart debezium
```

---

### Debezium connector status shows FAILED

```bash
curl http://localhost:8083/connectors/ticksense-symbol-config/status
```

Check the Debezium logs:
```bash
docker compose logs debezium | tail -50
```

Common root causes:
- Postgres not reachable: check `postgres` service health
- WAL level not logical: verify `-c wal_level=logical` in docker-compose.yml postgres command
- `postgres-init` not yet completed: symbol_config table doesn't exist yet

---

### Flink CDC job fails with "Unknown topic: postgres.public.symbol_config"

Cause: flink-init ran before debezium-init completed.

The `depends_on: debezium-init: condition: service_completed_successfully` in flink-init
should prevent this. If it still happens:
1. Check debezium-init logs: `docker compose logs debezium-init`
2. Manually re-submit: `make flink-submit`

---

### debezium/connect image tag not found on Docker Hub

```
Error response from daemon: manifest for debezium/connect:2.7 not found
```

**Cause:** `debezium/connect:2.7` does not exist. The Debezium project uses its own
versioning cadence separate from Docker Hub availability.

**Fix:** Use `debezium/connect:2.6` in `docker-compose.yml`. To find the latest available tag:
```bash
docker pull debezium/connect:2.6   # verify this works first
```

---

### Flink SQL fails with `Encountered ". raw"` (reserved keyword)

```
ParseException: Encountered ". raw" at line N, column M.
```

**Cause:** `raw` is a reserved data type keyword in Flink SQL. Any reference to
`iceberg_cat.raw` in DDL/DML statements fails the parser.

**Fix:** Rename the Iceberg namespace from `raw` to `bronze`:
- `flink/jobs/sql/catalogs.sql`
- `flink/jobs/sql/cdc_symbol_config/sink_bronze.sql`
- `flink/jobs/sql/cdc_symbol_config/insert_bronze.sql`

Then rebuild Flink images (SQL is copied into the image, not volume-mounted):
```bash
docker compose build flink-jobmanager flink-taskmanager flink-init
make up
```

---

### debezium-init fails with exit 22 on re-run

```
service "debezium-init" didn't complete successfully: exit 22
```

**Cause:** curl exit 22 = HTTP 409 Conflict. POSTing to `/connectors` when the
connector already exists returns 409.

**Fix:** Make the registration idempotent — check for existence before POSTing:
```bash
if curl -sf http://debezium:8083/connectors/ticksense-symbol-config > /dev/null 2>&1; then
  echo "Connector already exists, skipping"
else
  curl -sf -X POST http://debezium:8083/connectors \
    -H "Content-Type: application/json" \
    -d @/config/connector.json
fi
```

---

### Normalized table not updated by UPDATE events (silent drop)

**Symptom:** Bronze table has `op=u` rows, but `normalized.symbol_config` never changes.

**Diagnosis:**
```bash
# Check the UPDATE message in Kafka
docker exec ticksense-redpanda-1 \
  rpk topic consume postgres.public.symbol_config --num 1 --offset -1 2>/dev/null | \
  python3 -c "import sys,json; m=json.load(sys.stdin); v=json.loads(m['value']); print('before:', v['before'])"
```

If `before: null` — root cause is **PostgreSQL REPLICA IDENTITY**.

**Cause:** Default PostgreSQL REPLICA IDENTITY (`DEFAULT`) only logs PK columns
in the WAL for UPDATE. Debezium emits `before=null` when the PK wasn't changed.
Flink's `debezium-json` connector processes `before` for `op=u` events; a null
before-row causes a NullPointerException that silently drops the entire record.
The Flink job remains RUNNING — no error is logged.

**Fix:**
```bash
docker exec ticksense-postgres-1 psql -U ticksense -d ticksense \
  -c "ALTER TABLE symbol_config REPLICA IDENTITY FULL;"
```

This is also added to `infra/docker/postgres/init.sql` for fresh starts.

After the fix, the next UPDATE will have a fully-populated `before` object in Kafka,
and Flink will correctly emit `UPDATE_BEFORE + UPDATE_AFTER` to the Iceberg sink.

---

### Normalized table not being upserted (rows just appending)

Cause: Iceberg table was created with format-version=1 instead of 2.

Check:
```bash
docker compose exec trino trino --execute \
  "SELECT table_name, format_version FROM iceberg.\"normalized\".\"$properties\" WHERE table_name='symbol_config'"
```

Fix: Drop and recreate the table (requires stopping the Flink CDC job first):
```bash
docker compose exec trino trino --execute \
  "DROP TABLE IF EXISTS iceberg.normalized.symbol_config"
# Then restart flink-init (which recreates the table with format-version=2):
docker compose restart flink-init
```

---

### debezium-json format error in Flink logs

```
DeserializationRuntimeException: Failed to deserialize CDC JSON
```

Cause: Kafka message format doesn't match what debezium-json format expects.

Debug:
1. Check a raw message: `rpk topic consume postgres.public.symbol_config -n 1`
2. Verify `value.converter.schemas.enable=false` in connector.json
3. Verify `debezium-json.schema-include=false` in source_normalized.sql

---

### Replay: "no offset ranges" for time window

```
replay.no_offset_ranges: nothing to replay for the requested window
```

Cause A: kafka_offset=-1 in Iceberg AND no records in Kafka for the time window.

- Wait for ingest to produce data, then retry with `--start-ts` set to the last hour.

Cause B: The Kafka broker's `log.message.timestamp.type` stores broker-append-time,
not producer-create-time.  `offsets_for_times()` uses the record timestamp stored by
the broker, which may differ from the event's `exchange_event_ts`.

---

### Replay: "kafka_offset=-1 in all rows" (expected in current state)

This is a known limitation of Phase 2: `normalize.py` uses `SimpleStringSchema`
which does not expose Kafka record metadata.

To fix (future work):
1. Implement `KafkaRecordDeserializationSchema` in Python (complex due to Java↔Python
   serialization via Pemja/cloudpickle).
2. OR: Switch the normalize source to Table API with `'format'='json'` and
   `'scan.startup.mode'='earliest-offset'` — Kafka metadata columns are available
   as `VIRTUAL` columns in Flink SQL Table API.

Until fixed, the replay CLI falls back to `Kafka offsets_for_times()` automatically.

---

## Reset procedure (full clean slate)

```bash
make down          # stops all services, removes volumes
make up            # restarts everything, re-runs all init containers
```

After `make up`:
- Debezium takes a fresh snapshot of symbol_config → 5 op=r events in Kafka
- Flink CDC job replays from earliest offset → re-populates raw + normalized Iceberg tables

---

## Iceberg time-travel on normalized.symbol_config

Since normalized.symbol_config uses Iceberg v2 equality deletes, you can time-travel
to see the table at any past snapshot:

```sql
-- List snapshots
SELECT * FROM iceberg.normalized."symbol_config$snapshots" ORDER BY committed_at DESC;

-- Query table at a specific snapshot
SELECT * FROM iceberg.normalized.symbol_config FOR VERSION AS OF <snapshot_id>;
```

This is useful to verify that a DELETE op (op=d from Debezium) correctly removed the row.
