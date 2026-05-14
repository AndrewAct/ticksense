import asyncio
import json
from datetime import UTC, datetime

import httpx
import structlog
import websockets
from pydantic import ValidationError

from .models import BinanceDepthEvent, BinanceDepthSnapshot, OrderBookEvent
from .orderbook import GapDetectedError, OrderBook
from .producer import KafkaOrderBookProducer
from .settings import Settings

log = structlog.get_logger()

_MAX_RETRIES = 10
_BACKOFF_BASE = 1.0
_BACKOFF_CAP = 60.0
_SNAPSHOT_TIMEOUT = 10.0
_RECV_POLL_TIMEOUT = 0.05  # seconds; used while buffering during snapshot fetch


class BinanceOrderBookClient:
    """
    Connects to a single Binance diff-depth stream, maintains local order book state
    for gap detection, and produces every valid diff event to Kafka.
    """

    def __init__(self, symbol: str, producer: KafkaOrderBookProducer, settings: Settings) -> None:
        self._symbol = symbol.lower()
        self._producer = producer
        self._settings = settings

    async def run(self) -> None:
        """Run forever, reconnecting on any error with exponential backoff."""
        retries = 0
        while True:
            try:
                await self._run_once()
                retries = 0
            except Exception as exc:
                retries += 1
                backoff = min(_BACKOFF_BASE * (2**retries), _BACKOFF_CAP)
                log.warning(
                    "ws_error_reconnecting",
                    symbol=self._symbol,
                    error=str(exc),
                    retry=retries,
                    backoff_s=backoff,
                )
                if retries > _MAX_RETRIES:
                    log.error("max_retries_exceeded", symbol=self._symbol)
                    raise
                await asyncio.sleep(backoff)

    async def _run_once(self) -> None:
        url = f"{self._settings.binance_ws_base_url}/{self._symbol}@depth"
        log.info("ws_connecting", symbol=self._symbol)

        async with websockets.connect(url) as ws:
            book = OrderBook(symbol=self._symbol)
            await self._initialize_book(ws, book)

            async for raw in ws:
                event = self._parse_event(raw)
                if event is None:
                    continue
                try:
                    book.apply(event)
                except GapDetectedError as exc:
                    log.warning("gap_detected_reconnecting", symbol=self._symbol, detail=str(exc))
                    raise  # triggers reconnect in run()
                await self._producer.produce(self._to_orderbook_event(event))

    async def _initialize_book(self, ws: object, book: OrderBook) -> None:
        """
        Fetch REST snapshot while buffering live WebSocket diffs, then apply them.
        Any gap in the buffer triggers GapDetectedError → caller reconnects.
        """
        buffer: list[BinanceDepthEvent] = []
        snapshot_task: asyncio.Task[BinanceDepthSnapshot] = asyncio.create_task(
            self._fetch_snapshot()
        )

        try:
            while not snapshot_task.done():
                try:
                    raw = await asyncio.wait_for(_ws_recv(ws), timeout=_RECV_POLL_TIMEOUT)
                    event = self._parse_event(raw)
                    if event is not None:
                        buffer.append(event)
                except TimeoutError:
                    pass
        except Exception:
            snapshot_task.cancel()
            raise

        snapshot = await snapshot_task
        book.initialize(snapshot, buffer)

    async def _fetch_snapshot(self) -> BinanceDepthSnapshot:
        url = f"{self._settings.binance_rest_base_url}/api/v3/depth"
        params = {"symbol": self._symbol.upper(), "limit": str(1000)}
        async with httpx.AsyncClient(timeout=_SNAPSHOT_TIMEOUT) as client:
            resp = await client.get(url, params=params)
            resp.raise_for_status()
            return BinanceDepthSnapshot.model_validate(resp.json())

    def _parse_event(self, raw: object) -> BinanceDepthEvent | None:
        try:
            data = json.loads(raw)  # type: ignore[arg-type]
            event = BinanceDepthEvent.model_validate(data)
            if event.event_type != "depthUpdate":
                return None
            return event
        except (json.JSONDecodeError, ValidationError) as exc:
            log.warning("parse_error", symbol=self._symbol, error=str(exc))
            return None

    def _to_orderbook_event(self, event: BinanceDepthEvent) -> OrderBookEvent:
        return OrderBookEvent(
            event_id=f"binance#{self._symbol}#{event.last_update_id}",
            exchange="binance",
            symbol=self._symbol,
            exchange_event_ts=datetime.fromtimestamp(event.event_time_ms / 1000, tz=UTC),
            ingest_ts=datetime.now(UTC),
            first_update_id=event.first_update_id,
            last_update_id=event.last_update_id,
            bids=event.bids,
            asks=event.asks,
        )


async def _ws_recv(ws: object) -> str | bytes:
    """Thin typed wrapper so mypy doesn't complain about the websockets Any return."""
    result: str | bytes = await ws.recv()  # type: ignore[attr-defined]
    return result
