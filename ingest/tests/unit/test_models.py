from datetime import UTC, datetime

import pytest
from pydantic import ValidationError

from ingest.models import BinanceDepthEvent, BinanceDepthSnapshot, DLQEvent, OrderBookEvent


def _raw_depth() -> dict[str, object]:
    return {
        "e": "depthUpdate",
        "E": 1_672_515_782_136,
        "s": "BTCUSDT",
        "U": 157,
        "u": 160,
        "b": [["50000.00", "1.000"]],
        "a": [["50001.00", "0.500"]],
    }


class TestBinanceDepthEvent:
    def test_valid_parse(self) -> None:
        event = BinanceDepthEvent.model_validate(_raw_depth())
        assert event.event_type == "depthUpdate"
        assert event.last_update_id == 160
        assert event.first_update_id == 157
        assert event.bids == [("50000.00", "1.000")]
        assert event.asks == [("50001.00", "0.500")]

    def test_alias_fields_required(self) -> None:
        # Missing 'u' (last_update_id) must raise
        bad = _raw_depth()
        del bad["u"]
        with pytest.raises(ValidationError):
            BinanceDepthEvent.model_validate(bad)

    def test_empty_bids_asks_allowed(self) -> None:
        raw = {**_raw_depth(), "b": [], "a": []}
        event = BinanceDepthEvent.model_validate(raw)
        assert event.bids == []
        assert event.asks == []


class TestBinanceDepthSnapshot:
    def test_valid_parse(self) -> None:
        raw = {
            "lastUpdateId": 500,
            "bids": [["49999.0", "2.0"]],
            "asks": [["50001.0", "1.5"]],
        }
        snap = BinanceDepthSnapshot.model_validate(raw)
        assert snap.last_update_id == 500
        assert snap.bids == [("49999.0", "2.0")]

    def test_missing_last_update_id_raises(self) -> None:
        with pytest.raises(ValidationError):
            BinanceDepthSnapshot.model_validate({"bids": [], "asks": []})


class TestOrderBookEvent:
    def _make(self) -> OrderBookEvent:
        return OrderBookEvent(
            event_id="binance#btcusdt#160",
            exchange="binance",
            symbol="btcusdt",
            exchange_event_ts=datetime(2023, 1, 1, tzinfo=UTC),
            ingest_ts=datetime(2023, 1, 1, tzinfo=UTC),
            first_update_id=157,
            last_update_id=160,
            bids=[("50000.0", "1.0")],
            asks=[("50001.0", "0.5")],
        )

    def test_roundtrip_json(self) -> None:
        event = self._make()
        restored = OrderBookEvent.model_validate_json(event.model_dump_json())
        assert restored.event_id == "binance#btcusdt#160"
        assert restored.last_update_id == 160

    def test_kafka_fields_default_none(self) -> None:
        event = self._make()
        assert event.kafka_topic is None
        assert event.kafka_partition is None
        assert event.kafka_offset is None

    def test_dedup_key_format(self) -> None:
        event = self._make()
        assert event.event_id == f"binance#{event.symbol}#{event.last_update_id}"


class TestDLQEvent:
    def test_roundtrip_json(self) -> None:
        dlq = DLQEvent(
            original_key="binance#btcusdt",
            original_payload='{"broken": true}',
            error_type="ValidationError",
            error_message="field missing",
            ingest_ts=datetime(2023, 6, 1, tzinfo=UTC),
        )
        restored = DLQEvent.model_validate_json(dlq.model_dump_json())
        assert restored.error_type == "ValidationError"
        assert restored.original_key == "binance#btcusdt"
