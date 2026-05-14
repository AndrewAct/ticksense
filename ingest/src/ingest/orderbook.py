import structlog

from .models import BinanceDepthEvent, BinanceDepthSnapshot

log = structlog.get_logger()


class GapDetectedError(Exception):
    """Raised when a sequence gap is detected in Binance depth update IDs."""


class OrderBook:
    """
    In-memory L2 order book following the Binance WebSocket diff-depth protocol.

    Algorithm:
      1. Fetch REST snapshot while buffering WebSocket diffs.
      2. Drop buffered diffs where last_update_id <= snapshot.last_update_id.
      3. Apply remaining diffs; raise GapDetectedError if first_update_id > last_update_id + 1.
      4. On GapDetectedError: caller must restart from step 1 (re-snapshot).
    """

    def __init__(self, symbol: str) -> None:
        self.symbol = symbol
        self.last_update_id: int = 0
        self.bids: dict[str, str] = {}
        self.asks: dict[str, str] = {}

    def initialize(self, snapshot: BinanceDepthSnapshot, buffer: list[BinanceDepthEvent]) -> None:
        """Apply snapshot then replay buffered diffs. Raises GapDetectedError on gap."""
        self.last_update_id = snapshot.last_update_id
        self.bids = dict(snapshot.bids)
        self.asks = dict(snapshot.asks)

        for event in buffer:
            if event.last_update_id <= self.last_update_id:
                continue  # stale; drop
            if event.first_update_id > self.last_update_id + 1:
                raise GapDetectedError(
                    f"{self.symbol}: gap in buffer — "
                    f"U={event.first_update_id} > lastUpdateId+1={self.last_update_id + 1}"
                )
            self._apply(event)

        log.info("orderbook_initialized", symbol=self.symbol, last_update_id=self.last_update_id)

    def apply(self, event: BinanceDepthEvent) -> None:
        """Apply a single diff event. Raises GapDetectedError on gap; silently drops stale."""
        if event.last_update_id <= self.last_update_id:
            return  # stale; safe to ignore
        if event.first_update_id > self.last_update_id + 1:
            raise GapDetectedError(
                f"{self.symbol}: gap — "
                f"U={event.first_update_id} > lastUpdateId+1={self.last_update_id + 1}"
            )
        self._apply(event)

    def _apply(self, event: BinanceDepthEvent) -> None:
        for price, qty in event.bids:
            if float(qty) == 0.0:
                self.bids.pop(price, None)
            else:
                self.bids[price] = qty
        for price, qty in event.asks:
            if float(qty) == 0.0:
                self.asks.pop(price, None)
            else:
                self.asks[price] = qty
        self.last_update_id = event.last_update_id
