from datetime import UTC, datetime
from typing import Any
from unittest.mock import AsyncMock, patch

import pytest
from httpx import ASGITransport, AsyncClient

from api.dependencies import get_client
from api.main import app
from api.models.liquidity import LiquidityResponse, SpreadResponse
from api.models.ohlcv import OHLCVBar, OHLCVResponse
from api.models.pipeline import PipelineLagItem, PipelineLagResponse
from api.models.symbols import SymbolItem, SymbolsResponse
from api.read_model import get_read_model
from api.routers.ohlcv import _cold_cache
from api.trino_client import TrinoClient

NOW = datetime(2024, 1, 1, 12, 0, 0, tzinfo=UTC)

_OHLCV_ROW: dict[str, Any] = {
    "exchange": "binance",
    "symbol": "BTCUSDT",
    "window_start": NOW,
    "window_end": NOW,
    "open_price": 50000.0,
    "high_price": 51000.0,
    "low_price": 49000.0,
    "close_price": 50500.0,
    "volume": 100.0,
    "vwap": 50200.0,
    "tick_count": 60,
    "first_ts": NOW,
    "last_ts": NOW,
}

_LIQUIDITY_ROW: dict[str, Any] = {
    "exchange": "binance",
    "symbol": "BTCUSDT",
    "latest_ts": NOW,
    "best_bid_price": 49990.0,
    "best_ask_price": 50010.0,
    "spread": 20.0,
    "mid_price": 50000.0,
    "spread_bps": 0.4,
    "best_bid_qty": 10.0,
    "best_ask_qty": 5.0,
    "imbalance": 0.67,
    "market_signal": "BUY_PRESSURE",
    "staleness_seconds": 5,
    "freshness_status": "FRESH",
}

_HEALTH_ROW: dict[str, Any] = {
    "exchange": "binance",
    "symbol": "BTCUSDT",
    "latest_event_ts": NOW,
    "latest_ingest_ts": NOW,
    "staleness_seconds": 5,
    "freshness_status": "FRESH",
    "health_score": 1.0,
}

_SYMBOL_ROW: dict[str, Any] = {
    "symbol": "BTCUSDT",
    "exchange": "binance",
    "status": "TRADING",
    "base_asset": "BTC",
    "quote_asset": "USDT",
    "updated_at": NOW,
}


def _mock_client(rows: list[dict[str, Any]]) -> TrinoClient:
    client = TrinoClient()
    client.fetch = AsyncMock(return_value=rows)  # type: ignore[method-assign]
    return client


@pytest.fixture(autouse=True)
def setup_read_model():
    """Pre-populate the read model before each test and reset it after.

    This replaces the old pattern of dependency-overriding get_client() for
    every endpoint test.  Endpoints that still use Trino (OHLCV cold path)
    continue to use app.dependency_overrides within their own tests.
    """
    model = get_read_model()
    model.ready = True
    model.spread["BTCUSDT"] = SpreadResponse.model_validate(_LIQUIDITY_ROW)
    model.liquidity["BTCUSDT"] = LiquidityResponse.model_validate(_LIQUIDITY_ROW)
    model.ohlcv["BTCUSDT"] = OHLCVResponse(
        symbol="BTCUSDT",
        exchange="binance",
        bars=[OHLCVBar.model_validate(_OHLCV_ROW)],
        count=1,
    )
    model.pipeline = PipelineLagResponse(
        items=[PipelineLagItem.model_validate(_HEALTH_ROW)],
        checked_at=NOW,
        healthy_count=1,
        total_count=1,
    )
    model.symbols = SymbolsResponse(
        symbols=[SymbolItem.model_validate(_SYMBOL_ROW)],
        count=1,
    )
    _cold_cache.clear()
    yield
    model.ready = False
    model.spread.clear()
    model.liquidity.clear()
    model.ohlcv.clear()
    model.pipeline = None
    model.symbols = None
    _cold_cache.clear()


@pytest.fixture
def http_client():
    return AsyncClient(transport=ASGITransport(app=app), base_url="http://test")


class TestHealthEndpoints:
    async def test_health(self, http_client: AsyncClient) -> None:
        async with http_client as c:
            resp = await c.get("/health")
        assert resp.status_code == 200
        assert resp.json() == {"status": "ok"}

    async def test_ready_ok(self, http_client: AsyncClient) -> None:
        with patch("api.main.get_client", return_value=_mock_client([{"_col0": 1}])):
            async with http_client as c:
                resp = await c.get("/ready")
        assert resp.status_code == 200

    async def test_ready_trino_down(self, http_client: AsyncClient) -> None:
        bad_client = TrinoClient()
        bad_client.fetch = AsyncMock(side_effect=Exception("connection refused"))  # type: ignore[method-assign]
        with patch("api.main.get_client", return_value=bad_client):
            async with http_client as c:
                resp = await c.get("/ready")
        assert resp.status_code == 503


class TestOHLCVEndpoint:
    async def test_get_ohlcv(self, http_client: AsyncClient) -> None:
        async with http_client as c:
            resp = await c.get("/ohlcv/btcusdt")
        assert resp.status_code == 200
        body = resp.json()
        assert body["symbol"] == "BTCUSDT"
        assert body["count"] == 1
        assert body["interval"] == "1m"

    async def test_get_ohlcv_not_found(self, http_client: AsyncClient) -> None:
        async with http_client as c:
            resp = await c.get("/ohlcv/UNKNOWN")
        assert resp.status_code == 404

    async def test_get_ohlcv_symbol_uppercased(self, http_client: AsyncClient) -> None:
        # lowercase request → model is keyed uppercase → response symbol is uppercase
        async with http_client as c:
            resp = await c.get("/ohlcv/btcusdt")
        assert resp.status_code == 200
        assert resp.json()["symbol"] == "BTCUSDT"

    async def test_get_ohlcv_limit_slice(self, http_client: AsyncClient) -> None:
        # limit < preloaded bars should slice correctly
        model = get_read_model()
        bar = OHLCVBar.model_validate(_OHLCV_ROW)
        model.ohlcv["BTCUSDT"] = OHLCVResponse(
            symbol="BTCUSDT", exchange="binance", bars=[bar, bar, bar], count=3
        )
        async with http_client as c:
            resp = await c.get("/ohlcv/btcusdt?limit=2")
        assert resp.status_code == 200
        assert resp.json()["count"] == 2

    async def test_get_ohlcv_not_ready(self, http_client: AsyncClient) -> None:
        get_read_model().ready = False
        async with http_client as c:
            resp = await c.get("/ohlcv/btcusdt")
        assert resp.status_code == 503

    async def test_get_ohlcv_historical_range(self, http_client: AsyncClient) -> None:
        # from_ts triggers the cold (Trino) path; use params= so httpx encodes + correctly
        app.dependency_overrides[get_client] = lambda: _mock_client([_OHLCV_ROW])
        async with http_client as c:
            resp = await c.get("/ohlcv/btcusdt", params={"from_ts": NOW.isoformat()})
        app.dependency_overrides.clear()
        assert resp.status_code == 200
        assert resp.json()["count"] == 1

    async def test_get_ohlcv_historical_not_found(self, http_client: AsyncClient) -> None:
        app.dependency_overrides[get_client] = lambda: _mock_client([])
        async with http_client as c:
            resp = await c.get("/ohlcv/UNKNOWN", params={"from_ts": NOW.isoformat()})
        app.dependency_overrides.clear()
        assert resp.status_code == 404


class TestLiquidityEndpoints:
    async def test_get_spread(self, http_client: AsyncClient) -> None:
        async with http_client as c:
            resp = await c.get("/spread/btcusdt")
        assert resp.status_code == 200
        assert resp.json()["spread_bps"] == 0.4

    async def test_get_spread_not_found(self, http_client: AsyncClient) -> None:
        async with http_client as c:
            resp = await c.get("/spread/UNKNOWN")
        assert resp.status_code == 404

    async def test_get_spread_not_ready(self, http_client: AsyncClient) -> None:
        get_read_model().ready = False
        async with http_client as c:
            resp = await c.get("/spread/btcusdt")
        assert resp.status_code == 503

    async def test_get_liquidity(self, http_client: AsyncClient) -> None:
        async with http_client as c:
            resp = await c.get("/liquidity/btcusdt")
        assert resp.status_code == 200
        assert resp.json()["market_signal"] == "BUY_PRESSURE"


class TestPipelineEndpoint:
    async def test_get_pipeline_lag(self, http_client: AsyncClient) -> None:
        async with http_client as c:
            resp = await c.get("/pipeline/lag")
        assert resp.status_code == 200
        body = resp.json()
        assert body["total_count"] == 1
        assert body["healthy_count"] == 1

    async def test_pipeline_lag_empty(self, http_client: AsyncClient) -> None:
        get_read_model().pipeline = PipelineLagResponse(
            items=[], checked_at=NOW, healthy_count=0, total_count=0
        )
        async with http_client as c:
            resp = await c.get("/pipeline/lag")
        assert resp.status_code == 200
        assert resp.json()["total_count"] == 0

    async def test_pipeline_not_ready(self, http_client: AsyncClient) -> None:
        get_read_model().ready = False
        async with http_client as c:
            resp = await c.get("/pipeline/lag")
        assert resp.status_code == 503


class TestSymbolsEndpoint:
    async def test_list_symbols(self, http_client: AsyncClient) -> None:
        async with http_client as c:
            resp = await c.get("/symbols")
        assert resp.status_code == 200
        body = resp.json()
        assert body["count"] == 1
        assert body["symbols"][0]["symbol"] == "BTCUSDT"

    async def test_symbols_not_ready(self, http_client: AsyncClient) -> None:
        get_read_model().ready = False
        async with http_client as c:
            resp = await c.get("/symbols")
        assert resp.status_code == 503
