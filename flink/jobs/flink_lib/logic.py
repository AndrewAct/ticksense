"""
Pure Python business logic for Flink jobs.

No PyFlink imports — fully testable under Python 3.12 in the standard uv workspace.
All functions operate on plain dicts/lists; the Flink job wrappers call these
after reading from Flink state and before emitting output rows.
"""

from __future__ import annotations

from dataclasses import dataclass

# ── Order book helpers ────────────────────────────────────────────────────────


def apply_depth_diff(
    bids: dict[str, str],
    asks: dict[str, str],
    bid_updates: list[list[str]],
    ask_updates: list[list[str]],
) -> None:
    """
    Apply a Binance diff event to the in-memory order book (mutates in place).

    A level with qty == "0" is a deletion; all others are upserts.
    """
    for level in bid_updates:
        price, qty = level[0], level[1]
        if float(qty) == 0.0:
            bids.pop(price, None)
        else:
            bids[price] = qty
    for level in ask_updates:
        price, qty = level[0], level[1]
        if float(qty) == 0.0:
            asks.pop(price, None)
        else:
            asks[price] = qty


def best_bid(bids: dict[str, str]) -> tuple[float, float] | None:
    """Return (price, qty) of the best (highest-price) bid, or None if empty."""
    if not bids:
        return None
    price_str = max(bids, key=float)
    return float(price_str), float(bids[price_str])


def best_ask(asks: dict[str, str]) -> tuple[float, float] | None:
    """Return (price, qty) of the best (lowest-price) ask, or None if empty."""
    if not asks:
        return None
    price_str = min(asks, key=float)
    return float(price_str), float(asks[price_str])


def compute_spread(bb_price: float, ba_price: float) -> float:
    return ba_price - bb_price


def compute_mid_price(bb_price: float, ba_price: float) -> float:
    return (bb_price + ba_price) / 2.0


def compute_imbalance(bids: dict[str, str], asks: dict[str, str], depth: int = 5) -> float:
    """
    Top-N bid/ask volume imbalance: bid_vol / (bid_vol + ask_vol).

    Returns 0.5 (neutral) when both sides are empty.
    """
    sorted_bids = sorted(bids.items(), key=lambda kv: float(kv[0]), reverse=True)[:depth]
    sorted_asks = sorted(asks.items(), key=lambda kv: float(kv[0]))[:depth]
    bid_vol = sum(float(qty) for _, qty in sorted_bids)
    ask_vol = sum(float(qty) for _, qty in sorted_asks)
    total = bid_vol + ask_vol
    return bid_vol / total if total > 0 else 0.5


def dedup_key(exchange: str, symbol: str, last_update_id: int) -> str:
    """Canonical dedup key for an L2 diff event."""
    return f"{exchange}#{symbol}#{last_update_id}"


# ── OHLCV accumulator ─────────────────────────────────────────────────────────


@dataclass
class OHLCVState:
    """
    Accumulates per-tick mid-price data into OHLCV + VWAP for one (exchange, symbol, window).

    Uses best_bid_qty as a volume proxy — we work with L2 book ticks, not trades.
    """

    open_price: float = 0.0
    high_price: float = float("-inf")
    low_price: float = float("inf")
    close_price: float = 0.0
    volume: float = 0.0
    vwap_numerator: float = 0.0
    tick_count: int = 0
    first_ts_ms: int = 0
    last_ts_ms: int = 0

    def update(self, mid_price: float, bid_qty: float, ts_ms: int) -> None:
        if self.tick_count == 0:
            self.open_price = mid_price
            self.first_ts_ms = ts_ms
        self.high_price = max(self.high_price, mid_price)
        self.low_price = min(self.low_price, mid_price)
        self.close_price = mid_price
        self.volume += bid_qty
        self.vwap_numerator += mid_price * bid_qty
        self.tick_count += 1
        self.last_ts_ms = max(self.last_ts_ms, ts_ms)

    @property
    def vwap(self) -> float:
        return self.vwap_numerator / self.volume if self.volume > 0 else self.close_price

    @classmethod
    def from_tick(cls, mid_price: float, bid_qty: float, ts_ms: int) -> OHLCVState:
        state = cls()
        state.update(mid_price, bid_qty, ts_ms)
        return state
