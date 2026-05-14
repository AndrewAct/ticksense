import pytest

from ingest.models import BinanceDepthEvent, BinanceDepthSnapshot
from ingest.orderbook import GapDetectedError, OrderBook


def _event(
    first: int,
    last: int,
    bids: list[tuple[str, str]] | None = None,
    asks: list[tuple[str, str]] | None = None,
) -> BinanceDepthEvent:
    return BinanceDepthEvent.model_validate(
        {
            "e": "depthUpdate",
            "E": 1_000_000,
            "s": "BTCUSDT",
            "U": first,
            "u": last,
            "b": bids or [],
            "a": asks or [],
        }
    )


def _snapshot(last_update_id: int) -> BinanceDepthSnapshot:
    return BinanceDepthSnapshot.model_validate(
        {
            "lastUpdateId": last_update_id,
            "bids": [["50000.0", "1.0"], ["49999.0", "2.0"]],
            "asks": [["50001.0", "1.5"], ["50002.0", "0.5"]],
        }
    )


class TestInitialize:
    def test_snapshot_loads_book(self) -> None:
        book = OrderBook("btcusdt")
        book.initialize(_snapshot(100), [])
        assert book.last_update_id == 100
        assert book.bids["50000.0"] == "1.0"
        assert book.asks["50001.0"] == "1.5"

    def test_stale_buffered_events_dropped(self) -> None:
        book = OrderBook("btcusdt")
        # event.u=99 <= snapshot.lastUpdateId=100 → drop
        book.initialize(_snapshot(100), [_event(90, 99)])
        assert book.last_update_id == 100

    def test_valid_buffered_event_applied(self) -> None:
        book = OrderBook("btcusdt")
        book.initialize(
            _snapshot(100),
            [_event(101, 103, bids=[("50000.0", "3.0")])],
        )
        assert book.last_update_id == 103
        assert book.bids["50000.0"] == "3.0"

    def test_gap_in_buffer_raises(self) -> None:
        book = OrderBook("btcusdt")
        # first_update_id=103 > last_update_id+1=101 → gap
        with pytest.raises(GapDetectedError):
            book.initialize(_snapshot(100), [_event(103, 105)])

    def test_contiguous_buffer_events_applied_in_order(self) -> None:
        book = OrderBook("btcusdt")
        events = [
            _event(101, 102, bids=[("50000.0", "5.0")]),
            _event(103, 104, bids=[("50000.0", "7.0")]),
        ]
        book.initialize(_snapshot(100), events)
        assert book.last_update_id == 104
        assert book.bids["50000.0"] == "7.0"


class TestApply:
    def setup_method(self) -> None:
        self.book = OrderBook("btcusdt")
        self.book.initialize(_snapshot(100), [])

    def test_sequential_diff_applied(self) -> None:
        self.book.apply(_event(101, 102, asks=[("50001.0", "2.5")]))
        assert self.book.last_update_id == 102
        assert self.book.asks["50001.0"] == "2.5"

    def test_stale_event_silently_ignored(self) -> None:
        self.book.apply(_event(90, 99))
        assert self.book.last_update_id == 100  # unchanged

    def test_gap_raises(self) -> None:
        with pytest.raises(GapDetectedError):
            self.book.apply(_event(103, 105))

    def test_zero_qty_removes_bid_level(self) -> None:
        assert "50000.0" in self.book.bids
        self.book.apply(_event(101, 101, bids=[("50000.0", "0.00000000")]))
        assert "50000.0" not in self.book.bids

    def test_zero_qty_removes_ask_level(self) -> None:
        assert "50001.0" in self.book.asks
        self.book.apply(_event(101, 101, asks=[("50001.0", "0.00000000")]))
        assert "50001.0" not in self.book.asks

    def test_new_price_level_added(self) -> None:
        self.book.apply(_event(101, 101, bids=[("49998.0", "10.0")]))
        assert self.book.bids["49998.0"] == "10.0"

    def test_idempotent_replay(self) -> None:
        """Applying the same event twice (first time valid, second stale) is safe."""
        self.book.apply(_event(101, 101, bids=[("50000.0", "5.0")]))
        # second apply is stale (u=101 <= last_update_id=101)
        self.book.apply(_event(101, 101, bids=[("50000.0", "9.0")]))
        assert self.book.bids["50000.0"] == "5.0"  # second apply dropped
