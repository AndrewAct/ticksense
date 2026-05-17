import asyncio
from collections.abc import AsyncGenerator
from contextlib import asynccontextmanager

import structlog
from fastapi import FastAPI
from fastapi.responses import JSONResponse
from starlette.requests import Request

from .config import settings
from .dependencies import get_client
from .metrics import PrometheusMiddleware, metrics_endpoint
from .poller import run_poller
from .routers import liquidity, ohlcv, pipeline, symbols

structlog.configure(wrapper_class=structlog.make_filtering_bound_logger(settings.log_level))
log = structlog.get_logger()


@asynccontextmanager
async def lifespan(_: FastAPI) -> AsyncGenerator[None, None]:
    task = asyncio.create_task(run_poller(get_client()))
    try:
        yield
    finally:
        task.cancel()


app = FastAPI(
    title="TickSense API",
    description="Real-time crypto market analytics over Iceberg + Trino.",
    version="0.1.0",
    lifespan=lifespan,
)

app.add_middleware(PrometheusMiddleware)

app.include_router(ohlcv.router)
app.include_router(liquidity.router)
app.include_router(pipeline.router)
app.include_router(symbols.router)
app.add_route("/metrics", metrics_endpoint)


@app.get("/health", tags=["ops"])
async def health() -> JSONResponse:
    return JSONResponse({"status": "ok"})


@app.get("/ready", tags=["ops"])
async def ready() -> JSONResponse:
    if await get_client().ping():
        return JSONResponse({"status": "ready"})
    return JSONResponse({"status": "unavailable"}, status_code=503)


@app.exception_handler(Exception)
async def unhandled_exception_handler(request: Request, exc: Exception) -> JSONResponse:
    log.error("unhandled_exception", path=request.url.path, error=str(exc))
    return JSONResponse({"detail": "Internal server error"}, status_code=500)
