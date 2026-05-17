from datetime import UTC, datetime
from typing import Any
from unittest.mock import AsyncMock, patch

import pytest
from httpx import ASGITransport, AsyncClient

from api.dependencies import get_client
from api.main import app
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
        app.dependency_overrides[get_client] = lambda: _mock_client([_OHLCV_ROW])
        async with http_client as c:
            resp = await c.get("/ohlcv/btcusdt")
        app.dependency_overrides.clear()
        assert resp.status_code == 200
        body = resp.json()
        assert body["symbol"] == "BTCUSDT"
        assert body["count"] == 1
        assert body["interval"] == "1m"

    async def test_get_ohlcv_not_found(self, http_client: AsyncClient) -> None:
        app.dependency_overrides[get_client] = lambda: _mock_client([])
        async with http_client as c:
            resp = await c.get("/ohlcv/UNKNOWN")
        app.dependency_overrides.clear()
        assert resp.status_code == 404

    async def test_get_ohlcv_symbol_uppercased(self, http_client: AsyncClient) -> None:
        mock = _mock_client([_OHLCV_ROW])
        app.dependency_overrides[get_client] = lambda: mock
        async with http_client as c:
            await c.get("/ohlcv/btcusdt")
        app.dependency_overrides.clear()
        call_args = mock.fetch.call_args  # type: ignore[union-attr]
        assert "BTCUSDT" in call_args.args[1]


class TestLiquidityEndpoints:
    async def test_get_spread(self, http_client: AsyncClient) -> None:
        app.dependency_overrides[get_client] = lambda: _mock_client([_LIQUIDITY_ROW])
        async with http_client as c:
            resp = await c.get("/spread/btcusdt")
        app.dependency_overrides.clear()
        assert resp.status_code == 200
        assert resp.json()["spread_bps"] == 0.4

    async def test_get_spread_not_found(self, http_client: AsyncClient) -> None:
        app.dependency_overrides[get_client] = lambda: _mock_client([])
        async with http_client as c:
            resp = await c.get("/spread/UNKNOWN")
        app.dependency_overrides.clear()
        assert resp.status_code == 404

    async def test_get_liquidity(self, http_client: AsyncClient) -> None:
        app.dependency_overrides[get_client] = lambda: _mock_client([_LIQUIDITY_ROW])
        async with http_client as c:
            resp = await c.get("/liquidity/btcusdt")
        app.dependency_overrides.clear()
        assert resp.status_code == 200
        assert resp.json()["market_signal"] == "BUY_PRESSURE"


class TestPipelineEndpoint:
    async def test_get_pipeline_lag(self, http_client: AsyncClient) -> None:
        app.dependency_overrides[get_client] = lambda: _mock_client([_HEALTH_ROW])
        async with http_client as c:
            resp = await c.get("/pipeline/lag")
        app.dependency_overrides.clear()
        assert resp.status_code == 200
        body = resp.json()
        assert body["total_count"] == 1
        assert body["healthy_count"] == 1

    async def test_pipeline_lag_empty(self, http_client: AsyncClient) -> None:
        app.dependency_overrides[get_client] = lambda: _mock_client([])
        async with http_client as c:
            resp = await c.get("/pipeline/lag")
        app.dependency_overrides.clear()
        assert resp.status_code == 200
        assert resp.json()["total_count"] == 0


class TestSymbolsEndpoint:
    async def test_list_symbols(self, http_client: AsyncClient) -> None:
        app.dependency_overrides[get_client] = lambda: _mock_client([_SYMBOL_ROW])
        async with http_client as c:
            resp = await c.get("/symbols")
        app.dependency_overrides.clear()
        assert resp.status_code == 200
        body = resp.json()
        assert body["count"] == 1
        assert body["symbols"][0]["symbol"] == "BTCUSDT"
