# TickSense — End-to-End Debug Runbook

When something isn't working, verify each layer in order: **Kafka → Flink → Iceberg → Trino**.  
Each section has a "healthy" signal and a list of common failures.

---

## 1. Verify Kafka has data (ingest is running)

```bash
docker exec -it ticksense-redpanda-1 rpk topic consume market.raw.orderbook --num 1
```

**Healthy output:** a JSON message with `event_id`, `exchange`, `symbol`, `bids`, `asks`.

**If empty / timeout:** the ingest service is not running. Start it:
```bash
cd ingest && uv run python -m ingest.main
```

**Lessons learned:**
- `INGEST_SYMBOLS` in `.env` must be JSON array format: `["btcusdt","ethusdt"]`, not comma-separated.
- macOS needs an explicit SSL context for websockets — handled via `certifi` in `client.py`.
- Binance global endpoint (`stream.binance.com`) returns HTTP 451 from US IPs; use `stream.binance.us`.

---

## 2. Verify Flink jobs are running

```bash
curl -s http://localhost:8081/jobs | python3 -m json.tool
```

**Healthy output:** both normalize and ohlcv_1m jobs show `"status": "RUNNING"`.

**If a job shows FAILED:** get the Python traceback:
```bash
curl -s http://localhost:8081/jobs/<JOB_ID>/exceptions | python3 -c "
import json, sys
data = json.load(sys.stdin)
root = data.get('root-exception', '')
idx = root.find('Error received from SDK harness')
print(root[idx:idx+3000] if idx >= 0 else root[:3000])
"
```

**Common failures and fixes:**

| Error | Cause | Fix |
|---|---|---|
| `ModuleNotFoundError: No module named 'structlog'` | flink-init pickled `OrderBookProcessor` with structlog in its environment; taskmanager has a different image | Rebuild **all three** flink images together (see below) |
| `CustomPrint.print() got an unexpected keyword argument 'flush'` | structlog installed in flink image; PyFlink's Beam runner replaces `print()` with a version that doesn't accept `flush=True` | Remove structlog from flink Dockerfile; use standard `logging` |
| `Unable to load region from any of the providers` | AWS SDK v2's `DefaultAwsRegionProviderChain` ignores Iceberg/Flink config files; it only reads `AWS_REGION` env var | Add `AWS_REGION: us-east-1` to flink-jobmanager, flink-taskmanager, and flink-init in `docker-compose.yml` |
| `Connection reset by peer (Write failed)` during job submission | Docker Desktop disk full (BLOB server can't write temp files) | Run `docker system prune -f` to free space |
| `Recovery is suppressed by FailureRateRestartBackoffTimeStrategy` | Job failed 5 times in 5 minutes; Flink gave up | Fix the underlying error, then resubmit |

**Critical rule — always rebuild all three flink images together:**

When `flink run` submits a job, the **flink-init** container serializes `OrderBookProcessor` with cloudpickle. The serialized bytes are stored in the job graph and later deserialized by **flink-taskmanager**. If the two images have different Python packages installed, deserialization fails.

```bash
# Always rebuild all three at once
docker compose build --no-cache flink-jobmanager flink-taskmanager flink-init
```

**Restarting vs recreating containers:**

`docker compose restart` only restarts existing containers — it does **not** pick up new images.  
After a rebuild, always use:
```bash
docker compose up -d --force-recreate flink-jobmanager flink-taskmanager
```

**Resubmit jobs after recreating:**
```bash
docker compose run --rm flink-init
```

---

## 3. Verify Iceberg tables exist

```bash
docker exec -it ticksense-trino-1 trino --execute 'SHOW TABLES FROM iceberg.normalized'
```

**Healthy output:**
```
"book_ticker"
"ohlcv_1m"
```

**If tables are missing:** the Flink DDL never ran successfully. Check flink-init logs:
```bash
docker compose logs flink-init
```

---

## 4. Verify data reached Iceberg (via Trino)

```bash
docker exec -it ticksense-trino-1 trino --execute 'SELECT count(*) FROM iceberg.normalized.book_ticker'
```

**Healthy output:** a non-zero count.

**If count is 0 even though Flink is RUNNING:**  
Iceberg sinks write on checkpoint boundaries (every 60 seconds). Wait at least 60 seconds after the job starts before querying.  
Confirm checkpoints are happening:
```bash
docker compose logs --tail=20 flink-taskmanager | grep "checkpointId"
```

You should see lines like:
```
IcebergFilesCommitter - Start to flush snapshot state ... table: iceberg_cat.normalized.book_ticker, checkpointId: 3
```

---

## 5. Full reset procedure

If you need a clean restart from scratch:

```bash
# 1. Stop everything
docker compose down

# 2. Free Docker disk space (important after multiple --no-cache builds)
docker system prune -f

# 3. Rebuild flink images (all three)
docker compose build --no-cache flink-jobmanager flink-taskmanager flink-init

# 4. Start the stack
make up

# 5. Start ingest (in a separate terminal)
uv run python -m ingest.main

# 6. Submit Flink jobs
docker compose run --rm flink-init

# 7. Verify (after ~60s)
curl -s http://localhost:8081/jobs | python3 -m json.tool
docker exec -it ticksense-trino-1 trino --execute 'SELECT count(*) FROM iceberg.normalized.book_ticker'
```
