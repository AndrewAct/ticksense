---
name: production-python-service
description: Use when implementing any Python service: FastAPI endpoints, Kafka producers/consumers, CLI tools, data quality scripts.
---

# Production Python Service

## Requirements

- Python 3.12+
- Pydantic v2 for all data models; no untyped dicts at system boundaries
- pydantic-settings for configuration; all values from environment variables
- structlog for structured logging; never use `print()` or `logging.basicConfig`
- ruff for linting and formatting (line-length 100)
- mypy in strict mode
- No hardcoded values, secrets, or magic strings

## FastAPI specifics

- Async endpoints by default
- Request and response Pydantic models required on every endpoint
- `/health` and `/ready` on every service
- `/metrics` (Prometheus format) on every long-running service

## APIRouter structure (mandatory once project has >1 module)

```
src/<package>/
  main.py          # assembler only: middleware + include_router, no business logic
  dependencies.py  # shared Depends factories (get_client, get_db) — defined ONCE here
  read_model.py    # ReadModel dataclass + singleton (see OLAP section below)
  poller.py        # background task: single writer of ReadModel
  routers/
    users.py       # router = APIRouter(prefix="/users", tags=["users"])
    todos.py
  models/
    users.py       # Pydantic request/response models per domain
```

Rules:
- `main.py` only calls `app.include_router(...)` — no route definitions, no business logic
- `dependencies.py` is the single source of truth for all `Depends()` factories
- Client factories use `@lru_cache(maxsize=1)` so the same instance (and its connection pool) is reused across requests
- Each router imports `get_client` / `get_db` from `dependencies`, never defines its own
- Set `prefix` and `tags` on the `APIRouter`, not on `include_router` (keeps each file self-contained)
- For hot-path endpoints backed by ReadModel: test by populating the model; do NOT use `app.dependency_overrides`
- For cold-path (Trino) endpoints: test overrides via `app.dependency_overrides[get_client] = lambda: MockClient()`

```python
# dependencies.py
from functools import lru_cache
from .config import settings
from .trino_client import TrinoClient

@lru_cache(maxsize=1)
def get_client() -> TrinoClient:
    return TrinoClient(host=settings.trino_host, port=settings.trino_port, ...)

# routers/ohlcv.py — cold path still uses Depends; hot path reads from ReadModel
from ..dependencies import get_client

router = APIRouter(prefix="/ohlcv", tags=["ohlcv"])

@router.get("/{symbol}", response_model=OHLCVResponse)
async def get_ohlcv(symbol: str, client: TrinoClient = Depends(get_client)) -> OHLCVResponse:
    ...

# main.py
app.include_router(ohlcv.router)
```

---

## OLAP-backed API — ReadModel pattern

When the query layer sits on an OLAP engine (Trino, BigQuery, Spark SQL), every request
pays a minimum 50-100 ms query-planning floor regardless of query complexity.
The fix is a **poller-as-read-model**: one background task owns all writes to an in-memory
dict; request handlers only read from it.

### Decision tree for new endpoints

| Data type | Pattern | Latency |
|---|---|---|
| Current-state snapshot ("what is X right now?") | ReadModel — pre-loaded by poller | < 1 ms |
| Historical / arbitrary-parameter query | Trino cold path + `TTLCache` | 50–200 ms |
| Derivable from existing model data | Compute in router, no DB call | < 1 ms |

### File: `read_model.py`

```python
from dataclasses import dataclass, field

@dataclass
class ReadModel:
    # Always replace the whole dict (atomic under CPython GIL).
    # Never mutate an existing dict in place.
    spread: dict[str, SpreadResponse] = field(default_factory=dict)
    ohlcv: dict[str, OHLCVResponse] = field(default_factory=dict)
    pipeline: PipelineLagResponse | None = None
    ready: bool = False  # False until first poll cycle completes → endpoints return 503

_model = ReadModel()

def get_read_model() -> ReadModel:
    return _model
```

### File: `poller.py` — single writer

```python
async def _poll_something(client: TrinoClient) -> None:
    try:
        rows = await client.fetch(_SQL)
        # Build the new dict first, then swap atomically.
        _model.something = {str(r["key"]): Model.model_validate(r) for r in rows}
        log.info("poll_ok", count=len(rows))
    except Exception as exc:
        log.warning("poll_failed", error=str(exc))  # stale value stays; no crash

async def run_poller(client: TrinoClient) -> None:
    # Warm up before serving requests.
    await asyncio.gather(_poll_something(client), ..., return_exceptions=True)
    _model.ready = True

    last_slow = time.monotonic()
    while True:
        await asyncio.sleep(30)
        t = time.monotonic()
        tasks: list[Coroutine[Any, Any, None]] = [_poll_something(client)]
        if t - last_slow >= 300:          # slow data refreshed less often
            tasks.append(_poll_slow(client))
            last_slow = t
        await asyncio.gather(*tasks, return_exceptions=True)
```

### Hot-path router (reads model, zero Trino)

```python
@router.get("/spread/{symbol}", response_model=SpreadResponse)
async def get_spread(symbol: str) -> SpreadResponse:
    model = get_read_model()
    if not model.ready:
        raise HTTPException(status_code=503, detail="Service warming up")
    result = model.spread.get(symbol.upper())
    if result is None:
        raise HTTPException(status_code=404, detail=f"No data for {symbol}")
    return result
```

### Cold-path router (Trino + TTLCache for repeated queries)

```python
from cachetools import TTLCache
from threading import Lock

_cache: TTLCache[tuple, HistoryResponse] = TTLCache(maxsize=500, ttl=60)
_lock = Lock()

@router.get("/history/{symbol}", response_model=HistoryResponse)
async def get_history(
    symbol: str, from_ts: datetime, to_ts: datetime,
    client: TrinoClient = Depends(get_client),
) -> HistoryResponse:
    key = (symbol.upper(), from_ts, to_ts)
    with _lock:
        hit: HistoryResponse | None = _cache.get(key)
    if hit:
        return hit
    rows = await client.fetch(_SQL, [symbol, from_ts, to_ts])
    if not rows:
        raise HTTPException(404)
    result = HistoryResponse(...)
    with _lock:
        _cache[key] = result
    return result
```

### Test pattern

```python
@pytest.fixture(autouse=True)
def setup_read_model():
    model = get_read_model()
    model.ready = True
    model.spread["BTCUSDT"] = SpreadResponse(...)
    yield
    model.ready = False
    model.spread.clear()
    _cold_cache.clear()  # clear any TTLCache used by cold-path tests

# Hot-path test: populate model, no Trino mock needed.
async def test_get_spread(http_client):
    async with http_client as c:
        resp = await c.get("/spread/btcusdt")
    assert resp.status_code == 200

# Cold-path test: override the client, use params= for datetime URL encoding.
async def test_get_history(http_client):
    app.dependency_overrides[get_client] = lambda: _mock_client([_ROW])
    async with http_client as c:
        resp = await c.get("/history/btcusdt", params={"from_ts": NOW.isoformat()})
    app.dependency_overrides.clear()
    assert resp.status_code == 200
```

### Thread-safety rules

| Object | Safe? | Rule |
|---|---|---|
| `ReadModel` dict swap (`_model.x = new_dict`) | ✅ atomic under GIL | Never mutate in place |
| `ReadModel` field read (`.get(sym)`) | ✅ atomic under GIL | No lock needed |
| `cachetools.TTLCache` | ❌ not thread-safe | Always wrap with `threading.Lock` |
| `requests.Session` (Trino HTTP pool) | ✅ thread-safe | Share one instance via `@lru_cache` |

### Extending the model — checklist

Adding a new endpoint that needs fresh data:

1. Add a field to `ReadModel` in `read_model.py`
2. Write `_poll_<name>(client)` in `poller.py`
3. Register it in the initial `asyncio.gather` and the interval loop
4. Router reads `get_read_model().<field>.get(sym)` — no `Depends(get_client)` needed
5. Test: populate the field in `setup_read_model` fixture; no Trino mock required

Adding a new endpoint derivable from existing model data (no new poll needed):

- Just write the router. Read from existing model fields. Compute in Python.
- Example: `GET /top-movers` computes price change from `model.ohlcv[sym].bars`.

## Configuration pattern

```python
from pydantic_settings import BaseSettings

class Settings(BaseSettings):
    kafka_bootstrap_servers: str
    log_level: str = "INFO"

    model_config = {"env_file": ".env", "frozen": True}

settings = Settings()
```

## Logging pattern

```python
import structlog
log = structlog.get_logger()
log.info("event_produced", topic=topic, key=key, offset=offset)
```

## Anti-patterns to avoid

- `json.loads` / `json.dumps` without schema validation
- `dict` as a return type from functions that cross service boundaries
- `os.environ["KEY"]` instead of pydantic-settings
- Catching bare `Exception` without re-raising or logging with context
- Mutable default arguments or module-level mutable state
- Querying an OLAP engine on every request — always check the decision tree first
- Creating a new DB connection inside the request handler — use `@lru_cache` on the factory and share `requests.Session` across queries
- Mutating a `ReadModel` dict in place — always build a new dict and swap the reference atomically
- Using `TTLCache` without a `threading.Lock` — it is not thread-safe under concurrent async workers
- `datetime.isoformat()` in a URL query string without URL-encoding — the `+` in `+00:00` is parsed as a space; use `params={"key": value}` in httpx/requests so encoding is handled automatically
