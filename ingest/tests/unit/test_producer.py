"""Unit tests for KafkaOrderBookProducer using mocked aiokafka."""

from datetime import UTC, datetime
from unittest.mock import AsyncMock, MagicMock, patch

import pytest

from ingest.models import DLQEvent, OrderBookEvent
from ingest.producer import TOPIC_DLQ, TOPIC_RAW_ORDERBOOK, KafkaOrderBookProducer
from ingest.settings import Settings


def _settings() -> Settings:
    return Settings(kafka_bootstrap_servers="localhost:9092")


def _event(symbol: str = "btcusdt", last_update_id: int = 100) -> OrderBookEvent:
    return OrderBookEvent(
        event_id=f"binance#{symbol}#{last_update_id}",
        exchange="binance",
        symbol=symbol,
        exchange_event_ts=datetime(2024, 1, 1, tzinfo=UTC),
        ingest_ts=datetime(2024, 1, 1, tzinfo=UTC),
        first_update_id=last_update_id - 1,
        last_update_id=last_update_id,
        bids=[("50000.0", "1.0")],
        asks=[("50001.0", "0.5")],
    )


def _mock_record(topic: str = TOPIC_RAW_ORDERBOOK, partition: int = 0, offset: int = 1) -> object:
    r = MagicMock()
    r.topic = topic
    r.partition = partition
    r.offset = offset
    return r


@pytest.fixture
def mock_aiokafka() -> AsyncMock:
    with patch("ingest.producer.AIOKafkaProducer") as mock_cls:
        instance = AsyncMock()
        instance.start = AsyncMock()
        instance.stop = AsyncMock()
        instance.flush = AsyncMock()
        instance.send_and_wait = AsyncMock(return_value=_mock_record())
        mock_cls.return_value = instance
        yield instance


class TestKafkaProducerLifecycle:
    @pytest.mark.asyncio
    async def test_start_stop(self, mock_aiokafka: AsyncMock) -> None:
        p = KafkaOrderBookProducer(_settings())
        await p.start()
        mock_aiokafka.start.assert_called_once()
        await p.stop()
        mock_aiokafka.flush.assert_called_once()
        mock_aiokafka.stop.assert_called_once()

    @pytest.mark.asyncio
    async def test_stop_without_start_is_safe(self) -> None:
        p = KafkaOrderBookProducer(_settings())
        await p.stop()  # should not raise


class TestProducerProduce:
    @pytest.mark.asyncio
    async def test_happy_path(self, mock_aiokafka: AsyncMock) -> None:
        p = KafkaOrderBookProducer(_settings())
        await p.start()
        await p.produce(_event())
        mock_aiokafka.send_and_wait.assert_called_once()
        call_kwargs = mock_aiokafka.send_and_wait.call_args
        assert call_kwargs.args[0] == TOPIC_RAW_ORDERBOOK
        assert call_kwargs.kwargs["key"] == b"binance#btcusdt"

    @pytest.mark.asyncio
    async def test_message_key_format(self, mock_aiokafka: AsyncMock) -> None:
        p = KafkaOrderBookProducer(_settings())
        await p.start()
        await p.produce(_event(symbol="ethusdt"))
        call_kwargs = mock_aiokafka.send_and_wait.call_args
        assert call_kwargs.kwargs["key"] == b"binance#ethusdt"

    @pytest.mark.asyncio
    async def test_value_is_valid_json(self, mock_aiokafka: AsyncMock) -> None:
        p = KafkaOrderBookProducer(_settings())
        await p.start()
        event = _event()
        await p.produce(event)
        raw_value: bytes = mock_aiokafka.send_and_wait.call_args.kwargs["value"]
        consumed = OrderBookEvent.model_validate_json(raw_value)
        assert consumed.event_id == event.event_id

    @pytest.mark.asyncio
    async def test_assert_not_started_raises(self) -> None:
        p = KafkaOrderBookProducer(_settings())
        with pytest.raises(AssertionError):
            await p.produce(_event())


class TestProducerDLQ:
    @pytest.mark.asyncio
    async def test_send_to_dlq(self, mock_aiokafka: AsyncMock) -> None:
        p = KafkaOrderBookProducer(_settings())
        await p.start()
        await p._send_to_dlq(  # noqa: SLF001
            key="binance#btcusdt",
            payload="bad payload",
            error=ValueError("boom"),
        )
        # DLQ send_and_wait called
        call = mock_aiokafka.send_and_wait.call_args
        assert call.args[0] == TOPIC_DLQ
        dlq = DLQEvent.model_validate_json(call.kwargs["value"])
        assert dlq.original_key == "binance#btcusdt"
        assert dlq.error_type == "ValueError"
        assert dlq.error_message == "boom"

    @pytest.mark.asyncio
    async def test_dlq_send_failure_does_not_raise(self, mock_aiokafka: AsyncMock) -> None:
        mock_aiokafka.send_and_wait.side_effect = Exception("kafka down")
        p = KafkaOrderBookProducer(_settings())
        await p.start()
        # Should log and swallow
        await p._send_to_dlq(key="k", payload="p", error=ValueError("x"))  # noqa: SLF001
