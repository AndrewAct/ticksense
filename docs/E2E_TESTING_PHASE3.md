# Phase 3 — End-to-End Testing Guide

This document captures the complete journey of bringing up the Phase 3 stack,
every failure we hit, how we fixed it, and the final verification. Use it as a
runbook for reproducing or recording a demo.

---

## What Phase 3 Solves

Before Phase 3, the lakehouse had a blind spot: **slowly-changing reference data**.
Market tick data (prices, order books) flowed continuously through Kafka → Flink → Iceberg.
But trading-pair metadata — which symbols are active, their lot sizes, tick sizes, status —
lived only in Postgres with no path into the lakehouse.

**Pain points without Phase 3:**

| Problem | Impact |
|---|---|
| Downstream queries (Trino / dbt) can't join ticks with pair metadata | Mart models can't enrich OHLCV with lot-size context |
| Symbol goes from TRADING → BREAK in Postgres; analytics don't know | Wrong signal generation, e.g. liquidity score computed for a halted symbol |
| No audit trail of config changes | Can't answer "what was the tick size for BTCUSDT on 2026-05-10?" |
| Postgres is a single point of coupling | Every consumer queries Postgres directly; no replay, no time-travel |

**What Phase 3 delivers:**

1. **Debezium CDC** — captures every INSERT / UPDATE / DELETE on `symbol_config` via
   PostgreSQL WAL logical replication. Changes appear in Kafka within milliseconds.

2. **Dual Flink sinks:**
   - `bronze.symbol_config_cdc` — append-only audit log; every op code stored with WAL
     metadata (LSN, transaction ID, timestamps). Replay source.
   - `normalized.symbol_config` — Iceberg v2 upsert table; always reflects current state.
     Downstream dbt models read from here instead of hitting Postgres.

3. **Replay producer** — given a time window, re-produces raw orderbook events from Kafka
   back to the source topic. Useful for incident recovery after a downstream outage.

---

## Prerequisites

```bash
make lint typecheck test   # all must pass before bringing stack up
docker info                # Docker Desktop running
```

Port map (nothing else should bind these):

| Port | Service |
|---|---|
| 8080 | Redpanda Console |
| 8081 | Flink UI |
| 8082 | Trino |
| 8083 | Debezium Connect REST |
| 8181 | Iceberg REST catalog |
| 9000 | MinIO API |
| 9001 | MinIO Console |
| 9092 | Redpanda (internal) |
| 19092 | Redpanda (external / local dev) |
| 5432 | Postgres |

---

## Step 1 — Build Images

```bash
make build
```

This runs `docker compose build --no-cache` for all custom images:
- `ticksense-ingest` (Python ingest service)
- `ticksense-flink-jobmanager`
- `ticksense-flink-taskmanager`
- `ticksense-flink-init`

Expected tail output:
```
ticksense-ingest  Built
ticksense-flink-init  Built
ticksense-flink-taskmanager  Built
ticksense-flink-jobmanager  Built
```

**If you only changed SQL files or Python job files** (not the Dockerfile or Python deps),
you can skip the full `make build` and only rebuild Flink images:
```bash
docker compose build flink-jobmanager flink-taskmanager flink-init
```

### Failure encountered: debezium image tag not found

```
Error response from daemon: manifest for debezium/connect:2.7 not found
```

**Cause:** `debezium/connect:2.7` does not exist on Docker Hub.

**Fix:** Change `docker-compose.yml` debezium image to `debezium/connect:2.6`.

To check what tags are available:
```bash
docker pull debezium/connect:2.6   # verify this works
```

---

## Step 2 — Bring Up Stack

```bash
make up
```

This runs `docker compose up -d --wait` — starts all services and blocks until
every healthcheck passes.

Expected final output (all Healthy or Exited for one-shot init containers):
```
Container ticksense-redpanda-1         Healthy
Container ticksense-minio-1            Healthy
Container ticksense-postgres-1         Healthy
Container ticksense-iceberg-rest-1     Healthy
Container ticksense-flink-jobmanager-1 Healthy
Container ticksense-flink-taskmanager-1 Healthy
Container ticksense-flink-init-1       Healthy
Container ticksense-debezium-1         Healthy
Container ticksense-trino-1            Healthy
Container ticksense-ingest-1           Healthy
Container ticksense-redpanda-init-1    Exited   ← one-shot topic creator, normal
Container ticksense-minio-init-1       Exited   ← one-shot bucket creator, normal
Container ticksense-postgres-init-1    Exited   ← one-shot schema init, normal
Container ticksense-debezium-init-1    Exited   ← one-shot connector registration, normal
```

### Failure encountered: Flink SQL `raw` is a reserved keyword

**Symptom** (in `docker logs ticksense-flink-init-1`):
```
Caused by: ParseException: Encountered ". raw" at line 1, column 42.
```

**Cause:** `raw` is a reserved keyword in Flink SQL (it's a Flink data type name).
Any SQL reference to `iceberg_cat.raw` fails parsing. Affected files:
- `flink/jobs/sql/catalogs.sql`
- `flink/jobs/sql/cdc_symbol_config/sink_bronze.sql`
- `flink/jobs/sql/cdc_symbol_config/insert_bronze.sql`

**Fix:** Use backtick quoting: `` iceberg_cat.`raw` `` — OR rename the namespace
to `bronze` (recommended, avoids workaround entirely).

**Note:** This required rebuilding the Flink images (SQL files are `COPY`-ed into the image,
not volume-mounted):
```bash
docker compose build flink-jobmanager flink-taskmanager flink-init
make up
```

### Failure encountered: debezium-init not idempotent (exit 22)

**Symptom:**
```
service "debezium-init" didn't complete successfully: exit 22
```

**Cause:** curl exit 22 = HTTP 4xx. The `debezium-init` container POSTed to
`/connectors` to register the connector, but on re-runs the connector already
exists → HTTP 409 Conflict.

**Fix:** Make debezium-init check for existence before POSTing:
```yaml
command:
  - |
    if curl -sf http://debezium:8083/connectors/ticksense-symbol-config > /dev/null 2>&1; then
      echo "Connector already exists, skipping registration"
    else
      curl -sf -X POST http://debezium:8083/connectors \
        -H "Content-Type: application/json" \
        -d @/config/connector.json
    fi && \
    curl -sf http://debezium:8083/connectors/ticksense-symbol-config/status
```

---

## Step 3 — Verify Debezium Connector

```bash
curl -sf http://localhost:8083/connectors/ticksense-symbol-config/status | python3 -m json.tool
```

Expected:
```json
{
    "name": "ticksense-symbol-config",
    "connector": { "state": "RUNNING", "worker_id": "..." },
    "tasks": [{ "id": 0, "state": "RUNNING", "worker_id": "..." }],
    "type": "source"
}
```

---

## Step 4 — Verify CDC Topic Has Snapshot Data

Debezium automatically takes an initial snapshot of `symbol_config` on first start.
This produces 5 `op=r` (read/snapshot) events.

```bash
docker exec ticksense-redpanda-1 \
  rpk topic consume postgres.public.symbol_config --num 5 --offset start 2>/dev/null
```

Expected — 5 messages with `op=r` and `before=null`:
```json
{
  "topic": "postgres.public.symbol_config",
  "key": "{\"symbol\":\"BTCUSDT\"}",
  "value": "{\"before\":null,\"after\":{\"symbol\":\"BTCUSDT\",\"exchange\":\"binance\",\"status\":\"TRADING\",...},\"op\":\"r\",\"source\":{\"lsn\":26519528,...}}",
  "partition": 0,
  "offset": 0
}
```

---

## Step 5 — Verify Flink Jobs Running

```bash
curl -sf http://localhost:8081/jobs | python3 -m json.tool
```

Expected — 3 jobs, all RUNNING:
```json
{
    "jobs": [
        {"id": "...", "status": "RUNNING"},  ← normalize
        {"id": "...", "status": "RUNNING"},  ← ohlcv_1m
        {"id": "...", "status": "RUNNING"}   ← cdc_symbol_config
    ]
}
```

**Note:** If you query immediately after `make up`, the CDC job may not yet appear
(flink-init submits jobs sequentially). Wait ~10 seconds and re-query.

To see which job is which:
```bash
curl -sf http://localhost:8081/jobs/overview | \
  python3 -c "import sys,json; [print(j['name'][:60], j['state']) for j in json.load(sys.stdin)['jobs']]"
```

---

## Step 6 — Verify Iceberg Bronze Table

After the first Flink checkpoint (~60 seconds), the bronze table should have data.

```bash
docker exec ticksense-trino-1 trino --execute \
  "SELECT symbol, op, source_lsn FROM iceberg.bronze.symbol_config_cdc ORDER BY source_lsn LIMIT 10" \
  2>/dev/null
```

Expected:
```
"BTCUSDT","r","26519528"
"ETHUSDT","r","26519528"
"SOLUSDT","r","26519528"
"BNBUSDT","r","26519528"
"XRPUSDT","r","26519528"
```

---

## Step 7 — Verify Iceberg Normalized Table (Upsert)

```bash
docker exec ticksense-trino-1 trino --execute \
  "SELECT symbol, exchange, status FROM iceberg.normalized.symbol_config ORDER BY symbol" \
  2>/dev/null
```

Expected — 5 rows, current state only (not appended history):
```
"BNBUSDT","binance","TRADING"
"BTCUSDT","binance","TRADING"
"ETHUSDT","binance","TRADING"
"SOLUSDT","binance","TRADING"
"XRPUSDT","binance","TRADING"
```

---

## Step 8 — Test Live Update (Upsert Verification)

### Trigger an UPDATE in Postgres

```bash
docker exec ticksense-postgres-1 psql -U ticksense -d ticksense \
  -c "UPDATE symbol_config SET status='HALT' WHERE symbol='BTCUSDT';" 2>/dev/null
```

### Confirm CDC event arrived in Kafka

```bash
docker exec ticksense-redpanda-1 \
  rpk topic consume postgres.public.symbol_config --num 1 --offset -1 2>/dev/null
```

Expected — `op=u` with full `before` and `after`:
```json
{
  "value": "{\"before\":{\"symbol\":\"BTCUSDT\",\"status\":\"TRADING\",...},
             \"after\":{\"symbol\":\"BTCUSDT\",\"status\":\"HALT\",...},
             \"op\":\"u\",\"source\":{\"lsn\":26799304,...}}"
}
```

### Wait for Flink checkpoint (~60s), then verify upsert

```bash
docker exec ticksense-trino-1 trino --execute \
  "SELECT symbol, status FROM iceberg.normalized.symbol_config ORDER BY symbol" \
  2>/dev/null
```

Expected — BTCUSDT shows `HALT`, all others remain `TRADING`:
```
"BNBUSDT","TRADING"
"BTCUSDT","HALT"
"ETHUSDT","TRADING"
"SOLUSDT","TRADING"
"XRPUSDT","TRADING"
```

### Restore

```bash
docker exec ticksense-postgres-1 psql -U ticksense -d ticksense \
  -c "UPDATE symbol_config SET status='TRADING' WHERE symbol='BTCUSDT';" 2>/dev/null
```

---

## Troubleshooting: Normalized Table Not Updating

### Symptom

Bronze table picks up UPDATE events (`op=u` appears in `bronze.symbol_config_cdc`),
but `normalized.symbol_config` never reflects the change.

### Diagnosis

**Check 1 — Is the CDC job checkpointing?**

```bash
# Get the CDC job ID
JOB_ID=$(curl -sf http://localhost:8081/jobs | \
  python3 -c "import sys,json; jobs=json.load(sys.stdin)['jobs']; [print(j['id']) for j in jobs if j['status']=='RUNNING']" | head -1)

curl -sf "http://localhost:8081/jobs/${JOB_ID}/checkpoints" | \
  python3 -c "import sys,json; d=json.load(sys.stdin); print('completed:', d['counts']['completed'])"
```

**Check 2 — What do the Iceberg snapshots look like?**

```bash
docker exec ticksense-trino-1 trino --execute \
  'SELECT snapshot_id, committed_at, operation, summary
   FROM iceberg.normalized."symbol_config$snapshots"
   ORDER BY committed_at' 2>/dev/null
```

If you see only empty commits (no `added-data-files` in summary after the initial snapshot),
the debezium-json source is receiving messages but not emitting rows.

**Check 3 — What does the UPDATE message look like in Kafka?**

```bash
docker exec ticksense-redpanda-1 \
  rpk topic consume postgres.public.symbol_config --num 1 --offset -1 2>/dev/null | \
  python3 -c "import sys,json; m=json.load(sys.stdin); v=json.loads(m['value']); print('before:', v['before'])"
```

### Root Cause: `before=null` on UPDATE

**If `before` is `null`** for an `op=u` event, the root cause is PostgreSQL's default
**REPLICA IDENTITY**:

- `DEFAULT` (PostgreSQL default): for UPDATE, only logs primary-key columns of old row in WAL.
  Debezium emits `before=null` when the PK wasn't changed.
- Flink's `debezium-json` connector tries to process `before` for UPDATE events.
  A `null` before-row causes a NullPointerException that **silently drops the record**.
  The job stays RUNNING but no rows reach the Iceberg sink.

### Fix: Set REPLICA IDENTITY FULL

```bash
# Apply to running instance immediately:
docker exec ticksense-postgres-1 psql -U ticksense -d ticksense \
  -c "ALTER TABLE symbol_config REPLICA IDENTITY FULL;"
```

This change is already in `infra/docker/postgres/init.sql` so it will apply automatically
on future `make down && make up` cycles.

**Verify the fix** — after setting FULL, the next UPDATE message in Kafka will have a
fully-populated `before` object:
```json
{
  "before": {"symbol": "BTCUSDT", "status": "TRADING", ...},
  "after":  {"symbol": "BTCUSDT", "status": "HALT",    ...},
  "op": "u"
}
```

---

## Snapshot Inspection (Iceberg Time-Travel)

The normalized table uses Iceberg v2 equality deletes for upsert semantics.
You can inspect snapshots to verify what was committed:

```bash
# List all snapshots
docker exec ticksense-trino-1 trino --execute \
  'SELECT snapshot_id, committed_at, operation FROM iceberg.normalized."symbol_config$snapshots"
   ORDER BY committed_at' 2>/dev/null

# Query table at a specific past snapshot (time-travel)
docker exec ticksense-trino-1 trino --execute \
  'SELECT symbol, status FROM iceberg.normalized.symbol_config
   FOR VERSION AS OF <snapshot_id>
   ORDER BY symbol' 2>/dev/null
```

A healthy upsert cycle produces `operation=overwrite` snapshots (data + equality-delete files).
Empty checkpoints produce `operation=append` snapshots with no new files.

---

## Live Watch (for demos)

```bash
make watch-cdc
```

Polls `normalized.symbol_config` every 10 seconds and prints the current state.
While this runs, open another terminal and execute UPDATEs in Postgres to watch
changes propagate through the pipeline live.

---

## Full Reset

```bash
make down    # stops all containers and removes all volumes
make build   # rebuild images if SQL/Python files changed
make up      # fresh start
```

After reset: Debezium takes a new snapshot → 5 `op=r` events → Flink repopulates both tables.
