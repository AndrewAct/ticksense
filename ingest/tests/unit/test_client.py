"""Unit tests for BinanceOrderBookClient — pure logic, no network."""

import json
from datetime import UTC, datetime
from unittest.mock import AsyncMock, MagicMock

from ingest.client import BinanceOrderBookClient
from ingest.models import BinanceDepthEvent
from ingest.producer import KafkaOrderBookProducer
from ingest.settings import Settings


def _settings() -> Settings:
    return Settings(
        kafka_bootstrap_servers="localhost:9092",
        binance_ws_base_url="wss://stream.binance.com:9443/ws",
        binance_rest_base_url="https://api.binance.com",
    )


def _mock_producer() -> KafkaOrderBookProducer:
    p = MagicMock(spec=KafkaOrderBookProducer)
    p.produce = AsyncMock()
    return p


def _client(symbol: str = "btcusdt") -> BinanceOrderBookClient:
    return BinanceOrderBookClient(
        symbol=symbol,
        producer=_mock_producer(),
        settings=_settings(),
    )


def _raw_depth_msg(first: int = 101, last: int = 105) -> str:
    return json.dumps(
        {
            "e": "depthUpdate",
            "E": 1_672_515_782_136,
            "s": "BTCUSDT",
            "U": first,
            "u": last,
            "b": [["50000.0", "1.0"]],
            "a": [["50001.0", "0.5"]],
        }
    )


class TestParseEvent:
    def test_valid_depth_update(self) -> None:
        c = _client()
        result = c._parse_event(_raw_depth_msg())  # noqa: SLF001
        assert isinstance(result, BinanceDepthEvent)
        assert result.last_update_id == 105

    def test_wrong_event_type_returns_none(self) -> None:
        c = _client()
        msg = json.dumps({"e": "trade", "E": 123, "s": "BTCUSDT", "U": 1, "u": 2, "b": [], "a": []})
        assert c._parse_event(msg) is None  # noqa: SLF001

    def test_invalid_json_returns_none(self) -> None:
        c = _client()
        assert c._parse_event("not-json") is None  # noqa: SLF001

    def test_missing_field_returns_none(self) -> None:
        c = _client()
        assert c._parse_event(json.dumps({"e": "depthUpdate"})) is None  # noqa: SLF001

    def test_bytes_input_parsed(self) -> None:
        c = _client()
        result = c._parse_event(_raw_depth_msg().encode())  # noqa: SLF001
        assert result is not None


class TestToOrderBookEvent:
    def test_event_fields(self) -> None:
        c = _client()
        raw = BinanceDepthEvent.model_validate(json.loads(_raw_depth_msg(101, 105)))
        result = c._to_orderbook_event(raw)  # noqa: SLF001
        assert result.event_id == "binance#btcusdt#105"
        assert result.symbol == "btcusdt"
        assert result.exchange == "binance"
        assert result.first_update_id == 101
        assert result.last_update_id == 105
        assert result.bids == [("50000.0", "1.0")]
        assert result.asks == [("50001.0", "0.5")]

    def test_exchange_event_ts_from_ms(self) -> None:
        c = _client()
        raw = BinanceDepthEvent.model_validate(json.loads(_raw_depth_msg()))
        result = c._to_orderbook_event(raw)  # noqa: SLF001
        assert result.exchange_event_ts.tzinfo is not None
        assert result.exchange_event_ts == datetime.fromtimestamp(1_672_515_782_136 / 1000, tz=UTC)

    def test_symbol_normalized_lowercase(self) -> None:
        c = _client("ETHUSDT")
        raw = BinanceDepthEvent.model_validate(
            {
                "e": "depthUpdate",
                "E": 1000,
                "s": "ETHUSDT",
                "U": 1,
                "u": 2,
                "b": [],
                "a": [],
            }
        )
        result = c._to_orderbook_event(raw)  # noqa: SLF001
        assert result.symbol == "ethusdt"
        assert "ethusdt" in result.event_id
