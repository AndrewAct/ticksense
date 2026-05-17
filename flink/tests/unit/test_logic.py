"""Unit tests for flink/jobs/lib/logic.py — pure Python, no PyFlink dependency."""

import pytest
from flink_lib.logic import (
    OHLCVState,
    apply_depth_diff,
    best_ask,
    best_bid,
    compute_imbalance,
    compute_mid_price,
    compute_spread,
    dedup_key,
)

# ── apply_depth_diff ──────────────────────────────────────────────────────────


class TestApplyDepthDiff:
    def test_upsert_bid(self) -> None:
        bids: dict[str, str] = {}
        apply_depth_diff(bids, {}, [["50000.0", "1.5"]], [])
        assert bids == {"50000.0": "1.5"}

    def test_delete_bid_on_zero_qty(self) -> None:
        bids = {"50000.0": "1.5"}
        apply_depth_diff(bids, {}, [["50000.0", "0"]], [])
        assert bids == {}

    def test_upsert_ask(self) -> None:
        asks: dict[str, str] = {}
        apply_depth_diff({}, asks, [], [["50001.0", "2.0"]])
        assert asks == {"50001.0": "2.0"}

    def test_multiple_levels(self) -> None:
        bids: dict[str, str] = {"49999.0": "3.0"}
        apply_depth_diff(
            bids,
            {},
            [["50000.0", "1.5"], ["49999.0", "0"]],  # upsert + delete
            [],
        )
        assert bids == {"50000.0": "1.5"}

    def test_empty_updates_noop(self) -> None:
        bids = {"50000.0": "1.0"}
        asks = {"50001.0": "2.0"}
        apply_depth_diff(bids, asks, [], [])
        assert bids == {"50000.0": "1.0"}
        assert asks == {"50001.0": "2.0"}


# ── best_bid / best_ask ───────────────────────────────────────────────────────


class TestBestBidAsk:
    def test_best_bid_returns_highest_price(self) -> None:
        bids = {"49999.0": "1.0", "50000.0": "2.0", "49998.0": "3.0"}
        assert best_bid(bids) == (50000.0, 2.0)

    def test_best_ask_returns_lowest_price(self) -> None:
        asks = {"50002.0": "1.0", "50001.0": "2.0", "50003.0": "0.5"}
        assert best_ask(asks) == (50001.0, 2.0)

    def test_best_bid_empty_returns_none(self) -> None:
        assert best_bid({}) is None

    def test_best_ask_empty_returns_none(self) -> None:
        assert best_ask({}) is None

    def test_single_level(self) -> None:
        assert best_bid({"50000.0": "1.5"}) == (50000.0, 1.5)
        assert best_ask({"50001.0": "0.8"}) == (50001.0, 0.8)


# ── compute_spread / compute_mid_price ───────────────────────────────────────


class TestSpreadMid:
    def test_spread(self) -> None:
        assert compute_spread(50000.0, 50001.0) == pytest.approx(1.0)

    def test_mid_price(self) -> None:
        assert compute_mid_price(50000.0, 50002.0) == pytest.approx(50001.0)

    def test_zero_spread(self) -> None:
        assert compute_spread(50000.0, 50000.0) == pytest.approx(0.0)


# ── compute_imbalance ─────────────────────────────────────────────────────────


class TestImbalance:
    def test_equal_volume_returns_half(self) -> None:
        bids = {"50000.0": "1.0"}
        asks = {"50001.0": "1.0"}
        assert compute_imbalance(bids, asks) == pytest.approx(0.5)

    def test_all_bid_volume(self) -> None:
        bids = {"50000.0": "10.0"}
        asks = {"50001.0": "0.0000001"}  # negligible
        imbalance = compute_imbalance(bids, asks)
        assert imbalance > 0.99

    def test_empty_sides_returns_neutral(self) -> None:
        assert compute_imbalance({}, {}) == pytest.approx(0.5)

    def test_respects_depth_limit(self) -> None:
        # Only top-5 bids are counted even when more exist
        bids = {str(float(i)): "1.0" for i in range(100)}
        asks = {"99999.0": "5.0"}
        imbalance = compute_imbalance(bids, asks, depth=5)
        # bid_vol = 5.0, ask_vol = 5.0 → 0.5
        assert imbalance == pytest.approx(0.5)

    def test_bid_heavy_market(self) -> None:
        bids = {"50000.0": "8.0", "49999.0": "2.0"}  # 10 total
        asks = {"50001.0": "2.0", "50002.0": "2.0"}  # 4 total
        assert compute_imbalance(bids, asks) == pytest.approx(10 / 14)


# ── dedup_key ─────────────────────────────────────────────────────────────────


class TestDedupKey:
    def test_format(self) -> None:
        assert dedup_key("binance", "BTCUSDT", 123) == "binance#BTCUSDT#123"


# ── OHLCVState ────────────────────────────────────────────────────────────────


class TestOHLCVState:
    def test_first_tick_sets_open(self) -> None:
        s = OHLCVState.from_tick(50000.0, 1.0, 0)
        assert s.open_price == 50000.0
        assert s.close_price == 50000.0
        assert s.high_price == 50000.0
        assert s.low_price == 50000.0
        assert s.volume == 1.0
        assert s.tick_count == 1

    def test_high_low_tracked_correctly(self) -> None:
        s = OHLCVState.from_tick(50000.0, 1.0, 0)
        s.update(50100.0, 2.0, 10)
        s.update(49900.0, 0.5, 20)
        assert s.high_price == 50100.0
        assert s.low_price == 49900.0
        assert s.close_price == 49900.0

    def test_vwap_weighted_average(self) -> None:
        s = OHLCVState.from_tick(100.0, 2.0, 0)  # weight 2
        s.update(200.0, 3.0, 1)  # weight 3
        # vwap = (100*2 + 200*3) / (2+3) = 800/5 = 160
        assert s.vwap == pytest.approx(160.0)

    def test_vwap_falls_back_to_close_on_zero_volume(self) -> None:
        s = OHLCVState()
        s.open_price = 50000.0
        s.close_price = 50001.0
        assert s.vwap == pytest.approx(50001.0)

    def test_tick_count_increments(self) -> None:
        s = OHLCVState()
        for i in range(5):
            s.update(float(i), 1.0, i)
        assert s.tick_count == 5

    def test_timestamps_tracked(self) -> None:
        s = OHLCVState.from_tick(100.0, 1.0, 1_000)
        s.update(101.0, 1.0, 5_000)
        assert s.first_ts_ms == 1_000
        assert s.last_ts_ms == 5_000
