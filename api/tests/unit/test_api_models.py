from datetime import UTC, datetime

import pytest

from api.models.liquidity import LiquidityResponse, SpreadResponse
from api.models.ohlcv import OHLCVBar, OHLCVResponse
from api.models.pipeline import PipelineLagItem, PipelineLagResponse
from api.models.symbols import SymbolItem, SymbolsResponse

NOW = datetime(2024, 1, 1, 12, 0, 0, tzinfo=UTC)


def _bar() -> dict:
    return {
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


class TestOHLCVModels:
    def test_bar_valid(self) -> None:
        bar = OHLCVBar.model_validate(_bar())
        assert bar.symbol == "BTCUSDT"
        assert bar.tick_count == 60

    def test_response_count(self) -> None:
        bars = [OHLCVBar.model_validate(_bar())]
        resp = OHLCVResponse(symbol="BTCUSDT", exchange="binance", bars=bars, count=1)
        assert resp.interval == "1m"
        assert resp.count == 1

    def test_bar_missing_field(self) -> None:
        data = _bar()
        del data["open_price"]
        with pytest.raises(ValueError):
            OHLCVBar.model_validate(data)


class TestLiquidityModels:
    def test_spread_valid(self) -> None:
        data = {
            "exchange": "binance",
            "symbol": "BTCUSDT",
            "latest_ts": NOW,
            "best_bid_price": 49990.0,
            "best_ask_price": 50010.0,
            "spread": 20.0,
            "mid_price": 50000.0,
            "spread_bps": 0.4,
        }
        resp = SpreadResponse.model_validate(data)
        assert resp.spread_bps == 0.4

    def test_liquidity_market_signal(self) -> None:
        data = {
            "exchange": "binance",
            "symbol": "ETHUSDT",
            "latest_ts": NOW,
            "best_bid_price": 2999.0,
            "best_ask_price": 3001.0,
            "spread": 2.0,
            "mid_price": 3000.0,
            "spread_bps": 0.67,
            "best_bid_qty": 10.0,
            "best_ask_qty": 5.0,
            "imbalance": 0.67,
            "market_signal": "BUY_PRESSURE",
            "staleness_seconds": 5,
            "freshness_status": "FRESH",
        }
        resp = LiquidityResponse.model_validate(data)
        assert resp.market_signal == "BUY_PRESSURE"
        assert resp.freshness_status == "FRESH"


class TestPipelineModels:
    def test_lag_item(self) -> None:
        item = PipelineLagItem(
            exchange="binance",
            symbol="BTCUSDT",
            latest_event_ts=NOW,
            latest_ingest_ts=NOW,
            staleness_seconds=10,
            freshness_status="FRESH",
            health_score=1.0,
        )
        assert item.health_score == 1.0

    def test_response_counts(self) -> None:
        item = PipelineLagItem(
            exchange="binance",
            symbol="BTCUSDT",
            latest_event_ts=NOW,
            latest_ingest_ts=NOW,
            staleness_seconds=10,
            freshness_status="FRESH",
            health_score=1.0,
        )
        resp = PipelineLagResponse(
            items=[item],
            checked_at=NOW,
            healthy_count=1,
            total_count=1,
        )
        assert resp.total_count == 1


class TestSymbolModels:
    def test_symbol_item(self) -> None:
        item = SymbolItem(
            symbol="BTCUSDT",
            exchange="binance",
            status="TRADING",
            base_asset="BTC",
            quote_asset="USDT",
        )
        assert item.updated_at is None

    def test_symbols_response(self) -> None:
        item = SymbolItem(
            symbol="BTCUSDT",
            exchange="binance",
            status="TRADING",
            base_asset="BTC",
            quote_asset="USDT",
        )
        resp = SymbolsResponse(symbols=[item], count=1)
        assert resp.count == 1
