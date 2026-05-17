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
  routers/
    users.py       # router = APIRouter(prefix="/users", tags=["users"])
    todos.py
  models/
    users.py       # Pydantic request/response models per domain
```

Rules:
- `main.py` only calls `app.include_router(...)` — no route definitions, no business logic
- `dependencies.py` is the single source of truth for all `Depends()` factories
- Each router imports `get_client` / `get_db` from `dependencies`, never defines its own
- Set `prefix` and `tags` on the `APIRouter`, not on `include_router` (keeps each file self-contained)
- Test overrides via `app.dependency_overrides[get_client] = lambda: MockClient()`

```python
# dependencies.py
from .config import settings
from .trino_client import TrinoClient

def get_client() -> TrinoClient:
    return TrinoClient(host=settings.trino_host, port=settings.trino_port, ...)

# routers/ohlcv.py
from ..dependencies import get_client

router = APIRouter(prefix="/ohlcv", tags=["ohlcv"])

@router.get("/{symbol}", response_model=OHLCVResponse)
async def get_ohlcv(symbol: str, client: TrinoClient = Depends(get_client)) -> OHLCVResponse:
    ...

# main.py
app.include_router(ohlcv.router)
```

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
