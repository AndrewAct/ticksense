"""Async tests for BinanceOrderBookClient — mocked network I/O."""

import asyncio
import json
from unittest.mock import AsyncMock, MagicMock, patch

import pytest

from ingest.client import BinanceOrderBookClient, _ws_recv
from ingest.models import BinanceDepthSnapshot
from ingest.orderbook import GapDetectedError, OrderBook
from ingest.producer import KafkaOrderBookProducer
from ingest.settings import Settings


def _settings() -> Settings:
    return Settings(kafka_bootstrap_servers="localhost:9092")


def _mock_producer() -> KafkaOrderBookProducer:
    p = MagicMock(spec=KafkaOrderBookProducer)
    p.produce = AsyncMock()
    return p


def _client(symbol: str = "btcusdt") -> BinanceOrderBookClient:
    return BinanceOrderBookClient(symbol=symbol, producer=_mock_producer(), settings=_settings())


def _snapshot(last_update_id: int = 100) -> BinanceDepthSnapshot:
    return BinanceDepthSnapshot.model_validate(
        {
            "lastUpdateId": last_update_id,
            "bids": [["50000.0", "1.0"]],
            "asks": [["50001.0", "0.5"]],
        }
    )


def _fake_ws(messages: list[str]) -> object:
    """Return an async-iterable fake WebSocket that yields messages then stops."""

    class _Ws:
        _msgs = list(messages)

        def __aiter__(self) -> "_Ws":
            return self

        async def __anext__(self) -> str:
            if not self._msgs:
                raise StopAsyncIteration
            return self._msgs.pop(0)

    return _Ws()


def _ws_ctx(ws: object) -> AsyncMock:
    ctx = AsyncMock()
    ctx.__aenter__ = AsyncMock(return_value=ws)
    ctx.__aexit__ = AsyncMock(return_value=False)
    return ctx


class TestWsRecv:
    @pytest.mark.asyncio
    async def test_returns_message(self) -> None:
        ws = AsyncMock()
        ws.recv = AsyncMock(return_value="hello")
        assert await _ws_recv(ws) == "hello"

    @pytest.mark.asyncio
    async def test_returns_bytes(self) -> None:
        ws = AsyncMock()
        ws.recv = AsyncMock(return_value=b"bytes-msg")
        assert await _ws_recv(ws) == b"bytes-msg"


class TestFetchSnapshot:
    @pytest.mark.asyncio
    async def test_happy_path(self) -> None:
        c = _client()
        mock_resp = MagicMock()
        mock_resp.json.return_value = {
            "lastUpdateId": 500,
            "bids": [["49999.0", "2.0"]],
            "asks": [["50001.0", "1.5"]],
        }
        mock_resp.raise_for_status = MagicMock()

        ctx = AsyncMock()
        ctx.__aenter__ = AsyncMock(return_value=ctx)
        ctx.__aexit__ = AsyncMock(return_value=False)
        ctx.get = AsyncMock(return_value=mock_resp)

        with patch("ingest.client.httpx.AsyncClient", return_value=ctx):
            snap = await c._fetch_snapshot()  # noqa: SLF001

        assert snap.last_update_id == 500
        assert snap.bids == [("49999.0", "2.0")]

    @pytest.mark.asyncio
    async def test_http_error_propagates(self) -> None:
        c = _client()
        ctx = AsyncMock()
        ctx.__aenter__ = AsyncMock(return_value=ctx)
        ctx.__aexit__ = AsyncMock(return_value=False)
        ctx.get = AsyncMock(side_effect=Exception("connection refused"))

        with (
            patch("ingest.client.httpx.AsyncClient", return_value=ctx),
            pytest.raises(Exception, match="connection refused"),
        ):
            await c._fetch_snapshot()  # noqa: SLF001


class TestInitializeBook:
    @pytest.mark.asyncio
    async def test_empty_buffer(self) -> None:
        """Snapshot completes after one poll timeout; empty buffer applied."""
        c = _client()
        snap = _snapshot(100)

        async def instant_fetch() -> BinanceDepthSnapshot:
            return snap

        # Yield forever so wait_for's 0.05s timeout fires, letting instant_fetch complete.
        async def long_recv(ws: object) -> str | bytes:
            await asyncio.sleep(100)
            return ""  # unreachable

        with (
            patch.object(c, "_fetch_snapshot", instant_fetch),
            patch("ingest.client._ws_recv", long_recv),
        ):
            book = OrderBook("btcusdt")
            await c._initialize_book(MagicMock(), book)  # noqa: SLF001

        assert book.last_update_id == 100

    @pytest.mark.asyncio
    async def test_exception_from_ws_propagates(self) -> None:
        """RuntimeError from _ws_recv propagates and cancels the snapshot task."""
        c = _client()

        async def slow_fetch() -> BinanceDepthSnapshot:
            await asyncio.sleep(100)
            return _snapshot()  # unreachable

        async def boom_recv(ws: object) -> str | bytes:
            await asyncio.sleep(0)  # yield to let slow_fetch start
            raise RuntimeError("ws broke")

        with (
            patch.object(c, "_fetch_snapshot", slow_fetch),
            patch("ingest.client._ws_recv", boom_recv),
        ):
            book = OrderBook("btcusdt")
            with pytest.raises(RuntimeError, match="ws broke"):
                await c._initialize_book(MagicMock(), book)  # noqa: SLF001


class TestRunOnce:
    def _depth_msg(self, first: int = 101, last: int = 102) -> str:
        return json.dumps(
            {
                "e": "depthUpdate",
                "E": 1_000_000,
                "s": "BTCUSDT",
                "U": first,
                "u": last,
                "b": [["50000.0", "2.0"]],
                "a": [],
            }
        )

    @pytest.mark.asyncio
    async def test_processes_event_and_produces(self) -> None:
        c = _client()
        snap = _snapshot(100)

        async def mock_init(ws: object, book: OrderBook) -> None:
            book.initialize(snap, [])

        ws = _fake_ws([self._depth_msg(101, 102)])
        with (
            patch("ingest.client.websockets.connect", return_value=_ws_ctx(ws)),
            patch.object(c, "_initialize_book", mock_init),
        ):
            await c._run_once()

        assert c._producer.produce.call_count == 1  # type: ignore[attr-defined]

    @pytest.mark.asyncio
    async def test_none_event_skips_produce(self) -> None:
        c = _client()
        snap = _snapshot(100)

        async def mock_init(ws: object, book: OrderBook) -> None:
            book.initialize(snap, [])

        # Send a message that parses to None (wrong event type)
        bad_msg = json.dumps({"e": "trade", "E": 1, "s": "X", "U": 1, "u": 2, "b": [], "a": []})
        ws = _fake_ws([bad_msg])
        with (
            patch("ingest.client.websockets.connect", return_value=_ws_ctx(ws)),
            patch.object(c, "_initialize_book", mock_init),
        ):
            await c._run_once()

        assert c._producer.produce.call_count == 0  # type: ignore[attr-defined]

    @pytest.mark.asyncio
    async def test_gap_detected_propagates(self) -> None:
        c = _client()
        snap = _snapshot(100)

        async def mock_init(ws: object, book: OrderBook) -> None:
            book.initialize(snap, [])

        # U=103 > lastUpdateId+1=101 → gap
        gap_msg = self._depth_msg(first=103, last=105)
        ws = _fake_ws([gap_msg])
        with (
            patch("ingest.client.websockets.connect", return_value=_ws_ctx(ws)),
            patch.object(c, "_initialize_book", mock_init),
            pytest.raises(GapDetectedError),
        ):
            await c._run_once()


class TestRun:
    @pytest.mark.asyncio
    async def test_raises_after_max_retries(self) -> None:
        with patch("ingest.client._MAX_RETRIES", 2):
            c = _client()
            call_count = 0

            async def always_fails() -> None:
                nonlocal call_count
                call_count += 1
                raise ConnectionError("boom")

            with (
                patch.object(c, "_run_once", always_fails),
                patch("asyncio.sleep"),
                pytest.raises(ConnectionError),
            ):
                await c.run()

            assert call_count == 3  # initial + 2 retries

    @pytest.mark.asyncio
    async def test_retries_reset_on_success(self) -> None:
        """After a successful _run_once the retry counter resets to 0."""
        with patch("ingest.client._MAX_RETRIES", 1):
            c = _client()
            # fail → succeed (reset) → fail → fail (now retries=2 > 1 → raises)
            outcomes: list[Exception | None] = [
                ConnectionError("first"),
                None,
                ConnectionError("second"),
                ConnectionError("third"),
            ]
            idx = 0

            async def variable() -> None:
                nonlocal idx
                outcome = outcomes[idx]
                idx += 1
                if outcome is not None:
                    raise outcome

            with (
                patch.object(c, "_run_once", variable),
                patch("asyncio.sleep"),
                pytest.raises(ConnectionError, match="third"),
            ):
                await c.run()

            # All 4 outcomes consumed — proves counter reset after success
            assert idx == 4
