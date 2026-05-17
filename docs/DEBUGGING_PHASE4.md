# TickSense — Phase 4 Debug Runbook (API + dbt + Monitoring)

---

## dbt

### `dbt compile` fails with "Connection refused"

**Symptom:** `make dbt-compile` exits with `HTTPConnectionPool … Connection refused`

**Cause:** `dbt compile` with the Trino adapter tries to connect to Trino even for compilation. This is expected when the stack is down.

**Fix:** Start the stack first (`make up`), then run `make dbt-compile`.

**Note:** `dbt parse` and `dbt ls` work offline and can validate model graph without a live connection.

---

### `dbt source freshness` on `symbol_config` always skips

**Cause:** `symbol_config` has `freshness: null` in `sources.yml` — it's a CDC table updated on demand, not a streaming source.

**Expected:** Only `book_ticker` and `ohlcv_1m` are freshness-checked.

---

### dbt `period: second` not valid

**Symptom:** `Parsing Error … is not valid under any of the given schemas`

**Cause:** dbt freshness only supports `minute`, `hour`, `day` — not `second`.

**Fix:** Use `minute` as the smallest period unit in `sources.yml`.

---

### dbt marts not queryable from API

**Symptom:** API returns 500 / Trino reports `Table not found: iceberg.marts.mart_ohlcv`

**Cause:** dbt has not been run yet; the mart tables/views don't exist.

**Fix:**
```bash
make dbt-run   # creates staging views + mart tables/views in Trino
```

The `dbt-runner` service in docker-compose runs this automatically on `make up`.

---

## Docker

### API healthcheck fails — `curl: executable file not found`

**Symptom:** `docker inspect ticksense-api-1` shows health status `unhealthy`; prometheus and grafana never start (they depend on api being healthy).

**Cause:** `python:3.13-slim` is a stripped Debian image with no `curl`. The healthcheck `curl -sf http://localhost:8000/health` fails immediately.

**Fix:** Install `curl` in `api/Dockerfile`:
```dockerfile
RUN apt-get update && apt-get install -y --no-install-recommends curl \
    && rm -rf /var/lib/apt/lists/*
```

Rebuild the image (`docker compose build api`) after this change.

---

## API (FastAPI)

### `ModuleNotFoundError: No module named 'api'` in pytest

**Symptom:** Tests fail to collect with import error on `api`, `ingest`, or `replay`.

**Root cause:** uv workspace members must be installed as editable packages so pytest can find their source.

**Fix:** Run `uv sync --all-packages` (not just `uv sync`) to install all workspace member dependencies into the venv. Plain `uv sync` only syncs the root package. The editable installs place `.pth` files in site-packages that add each `src/` directory to `sys.path` automatically — no `pythonpath` pytest config needed.

> **Do NOT add `pythonpath` to `[tool.pytest.ini_options]`** — it duplicates the `.pth` setup and causes namespace-package shadowing. See the "namespace package shadowing" entry above.

---

### API dependencies (pydantic, structlog, etc.) not found in tests

**Symptom:** `ModuleNotFoundError: No module named 'pydantic'` when running `api/tests`.

**Cause:** `uv sync` without `--all-packages` does not install workspace member runtime dependencies.

**Fix:**
```bash
uv sync --all-packages
```

Add this to onboarding / CI setup steps.

---

### `/ready` returns 503 even when Trino is up

**Cause:** `TrinoClient.ping()` catches all exceptions silently. Check:
1. `TRINO_HOST` env var — defaults to `localhost`; inside Docker it should be `trino`
2. `TRINO_PORT` — defaults to `8082` (host-mapped); inside Docker it should be `8080`
3. Trino healthcheck: `curl -sf http://localhost:8082/v1/info | python3 -m json.tool`

---

### Prometheus `/metrics` returns 404

**Cause:** `/metrics` is registered via `app.add_route(...)` not `@app.get(...)` — it won't appear in OpenAPI docs but is accessible at `http://localhost:8000/metrics`.

---

## Dependency injection in tests

### Pattern: override `get_client` per test

```python
from api.dependencies import get_client
from api.main import app

def _mock_client(rows):
    client = TrinoClient()
    client.fetch = AsyncMock(return_value=rows)
    return client

app.dependency_overrides[get_client] = lambda: _mock_client([...])
# ... run test ...
app.dependency_overrides.clear()  # always clean up
```

### Pattern: test `/ready` (not a Depends endpoint)

`/ready` calls `get_client()` directly (not via `Depends`), so override via `patch`:
```python
with patch("api.main.get_client", return_value=_mock_client([{"_col0": 1}])):
    resp = await client.get("/ready")
```

---

## pytest: `No module named 'replay.config'` — namespace package shadowing

**Symptom:** Running `make test` or `make coverage` fails at collection with:
```
ImportError while loading conftest 'replay/tests/conftest.py'
ModuleNotFoundError: No module named 'replay.config'
```
Even though `uv run python -c "from replay.config import Settings"` works fine.

**Root cause (subtle):** Three layers interact badly:

1. In a uv workspace with `src/` layout, workspace member directories (`replay/`, `api/`, `ingest/`) share names with their Python packages but live at the project root *without* `__init__.py`.
2. Python's import system, upon seeing `replay/` in the current working directory (via `''` on `sys.path`), marks it as a **namespace package portion** and caches it in `sys.modules` as `_NamespacePath(['.../replay'])`.
3. When `--import-mode=importlib` and the `pythonpath` pytest config option are used *together*, pytest triggers this namespace-package import during its configuration phase — **before** the `pythonpath` plugin finishes inserting `replay/src` into `sys.path`. The stale namespace package is then cached and can't be evicted by later path insertions.

The crucial symptom: `replay.__file__` is `None` and `replay.__path__` is `_NamespacePath(['.../replay'])` — this is the workspace *member directory*, not the real package at `replay/src/replay/`.

**Why `uv run python` works but `uv run pytest` doesn't:**
`uv run python` imports `replay` lazily, after site-packages `.pth` files have already added `replay/src` to `sys.path`. Python scans `sys.path` in order, finds the namespace portion at `''` (cwd), continues scanning, and eventually finds the regular package at `replay/src/replay/` — regular package wins. But pytest's importlib mode triggers module discovery differently and hits the namespace package first.

**Fix applied:**

1. **Removed `pythonpath` from `[tool.pytest.ini_options]`** — the editable installs already handle this via `.pth` files in site-packages. The `pythonpath` config was redundant and its insertion timing broke the import order.
2. **Removed `--import-mode=importlib`** — not needed once `pythonpath` is gone.
3. **Removed `__init__.py` from `api/tests/`** — rootless test layout avoids module-name collisions between `api/tests/unit/test_models.py` and `ingest/tests/unit/test_models.py`.
4. **Renamed `api/tests/unit/test_models.py` → `test_api_models.py`** — eliminates the naming collision.

**Rule of thumb for uv workspaces:** Let editable installs (`.pth` files) own the import paths. Don't add `pythonpath` to pytest config — it duplicates the `.pth` setup and introduces timing races. Use rootless test layout (no `__init__.py` in test dirs) when multiple workspace members have tests with identical filenames.

---

## Grafana: Health Score oscillates between 100% and 50%

**Symptom:** `Health Score by Symbol` panel alternates between 100% and 50% every ~30 seconds.

**Root cause:** Flink writes to Iceberg at **checkpoint boundaries**, not per-event. With a ~60s checkpoint interval, the data visible to Trino lags 0–60s behind real time. The original `health_score` formula was:

```sql
WHEN staleness_seconds <= 30 THEN 1.0   -- FRESH
WHEN staleness_seconds <= 60 THEN 0.5   -- WARN
```

Because staleness grows from ~0s (just after checkpoint) to ~60s (just before next checkpoint), the score oscillates: `1.0 → 0.5 → 1.0 → …` in sync with the checkpoint cycle. This looks like a pipeline problem but is actually normal lakehouse behavior.

**Fix:** Align the threshold with the actual checkpoint interval:

```sql
WHEN staleness_seconds <= 60  THEN 1.0  -- FRESH (one full checkpoint window)
WHEN staleness_seconds <= 120 THEN 0.5  -- WARN
```

Files changed: `dbt/models/marts/mart_exchange_health.sql`, `dbt/models/intermediate/int_freshness_status.sql`, Grafana dashboard description + threshold annotation.

**Key insight:** In a lakehouse architecture, "freshness" is bounded by the streaming engine's commit interval, not by the source event rate. WebSocket events arrive every millisecond; Iceberg snapshots commit every 30–60s. Freshness SLOs must be set against the commit interval, not the ingestion rate.

---

## Port reference

| Service    | Host port |
|------------|-----------|
| FastAPI    | 8000      |
| Prometheus | 9090      |
| Grafana    | 3000      |
| Trino      | 8082      |
| MinIO UI   | 9001      |
| Flink UI   | 8081      |
| Redpanda   | 8080      |
