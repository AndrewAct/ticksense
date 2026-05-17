import time
from collections.abc import Awaitable, Callable

from prometheus_client import CONTENT_TYPE_LATEST, Counter, Histogram, generate_latest
from starlette.middleware.base import BaseHTTPMiddleware
from starlette.requests import Request
from starlette.responses import Response

REQUEST_COUNT: Counter = Counter(
    "api_requests_total",
    "Total HTTP requests",
    ["method", "endpoint", "status_code"],
)

REQUEST_DURATION: Histogram = Histogram(
    "api_request_duration_seconds",
    "HTTP request duration in seconds",
    ["method", "endpoint"],
    buckets=[0.01, 0.05, 0.1, 0.25, 0.5, 1.0, 2.5, 5.0],
)

_STATIC_SEGMENTS = {
    "ohlcv",
    "spread",
    "liquidity",
    "symbols",
    "pipeline",
    "lag",
    "health",
    "ready",
    "metrics",
}


def _normalize_path(path: str) -> str:
    parts = path.strip("/").split("/")
    return "/" + "/".join(p if p in _STATIC_SEGMENTS else "{param}" for p in parts)


class PrometheusMiddleware(BaseHTTPMiddleware):
    async def dispatch(
        self,
        request: Request,
        call_next: Callable[[Request], Awaitable[Response]],
    ) -> Response:
        endpoint = _normalize_path(request.url.path)
        start = time.perf_counter()
        response = await call_next(request)
        duration = time.perf_counter() - start

        REQUEST_COUNT.labels(
            method=request.method,
            endpoint=endpoint,
            status_code=str(response.status_code),
        ).inc()
        REQUEST_DURATION.labels(method=request.method, endpoint=endpoint).observe(duration)
        response.headers["X-Response-Time-Ms"] = f"{duration * 1000:.1f}"
        return response


async def metrics_endpoint(_: Request) -> Response:
    return Response(generate_latest(), media_type=CONTENT_TYPE_LATEST)
