"""Integration tests: KafkaOrderBookProducer against a real Kafka container."""

import asyncio
from datetime import UTC, datetime

import pytest
from aiokafka import AIOKafkaConsumer
from testcontainers.kafka import KafkaContainer  # type: ignore[import-untyped]

from ingest.models import OrderBookEvent
from ingest.producer import TOPIC_DLQ, TOPIC_RAW_ORDERBOOK, KafkaOrderBookProducer
from ingest.settings import Settings


@pytest.fixture(scope="module")
def redpanda() -> object:
    with KafkaContainer().with_kraft() as container:
        yield container


@pytest.fixture
def ingest_settings(redpanda: object) -> Settings:
    bootstrap = redpanda.get_bootstrap_server()  # type: ignore[attr-defined]
    return Settings(kafka_bootstrap_servers=bootstrap)


def _make_event(symbol: str = "btcusdt", last_update_id: int = 160) -> OrderBookEvent:
    return OrderBookEvent(
        event_id=f"binance#{symbol}#{last_update_id}",
        exchange="binance",
        symbol=symbol,
        exchange_event_ts=datetime(2024, 1, 1, tzinfo=UTC),
        ingest_ts=datetime(2024, 1, 1, tzinfo=UTC),
        first_update_id=last_update_id - 3,
        last_update_id=last_update_id,
        bids=[("50000.0", "1.0")],
        asks=[("50001.0", "0.5")],
    )


async def _consume_one(bootstrap: str, topic: str, timeout: float = 15.0) -> bytes:
    consumer = AIOKafkaConsumer(
        topic,
        bootstrap_servers=bootstrap,
        auto_offset_reset="earliest",
        group_id=f"test-{topic}-{id(bootstrap)}",
    )
    await consumer.start()
    try:
        msg = await asyncio.wait_for(consumer.getone(), timeout=timeout)
        return msg.value
    finally:
        await consumer.stop()


@pytest.mark.asyncio
async def test_produce_happy_path(ingest_settings: Settings) -> None:
    """Produced event can be consumed with correct key and payload."""
    producer = KafkaOrderBookProducer(ingest_settings)
    await producer.start()

    event = _make_event()
    await producer.produce(event)
    await producer.stop()

    consumer = AIOKafkaConsumer(
        TOPIC_RAW_ORDERBOOK,
        bootstrap_servers=ingest_settings.kafka_bootstrap_servers,
        auto_offset_reset="earliest",
        group_id="test-happy-path",
    )
    await consumer.start()
    try:
        msg = await asyncio.wait_for(consumer.getone(), timeout=15.0)
        assert msg.key == b"binance#btcusdt"
        consumed = OrderBookEvent.model_validate_json(msg.value)
        assert consumed.event_id == "binance#btcusdt#160"
        assert consumed.last_update_id == 160
    finally:
        await consumer.stop()


@pytest.mark.asyncio
async def test_idempotent_produce(ingest_settings: Settings) -> None:
    """Producing the same event twice writes two messages (broker-level dedup is Flink's job)."""
    producer = KafkaOrderBookProducer(ingest_settings)
    await producer.start()

    event = _make_event(symbol="ethusdt", last_update_id=200)
    await producer.produce(event)
    await producer.produce(event)
    await producer.stop()

    consumer = AIOKafkaConsumer(
        TOPIC_RAW_ORDERBOOK,
        bootstrap_servers=ingest_settings.kafka_bootstrap_servers,
        auto_offset_reset="earliest",
        group_id="test-idempotent",
    )
    await consumer.start()
    try:
        msgs: list[OrderBookEvent] = []
        for _ in range(10):
            try:
                msg = await asyncio.wait_for(consumer.getone(), timeout=3.0)
                parsed = OrderBookEvent.model_validate_json(msg.value)
                if parsed.symbol == "ethusdt":
                    msgs.append(parsed)
                    if len(msgs) >= 2:
                        break
            except TimeoutError:
                break
        assert len(msgs) == 2
        assert all(m.event_id == "binance#ethusdt#200" for m in msgs)
    finally:
        await consumer.stop()


@pytest.mark.asyncio
async def test_dlq_on_producer_stop_before_produce(ingest_settings: Settings) -> None:
    """Confirm DLQ topic is reachable (producer can write to it)."""
    producer = KafkaOrderBookProducer(ingest_settings)
    await producer.start()
    # Directly call the DLQ path
    await producer._send_to_dlq(  # noqa: SLF001
        key="binance#solusdt",
        payload='{"broken": true}',
        error=ValueError("test error"),
    )
    await producer.stop()

    value = await _consume_one(ingest_settings.kafka_bootstrap_servers, TOPIC_DLQ, timeout=15.0)
    from ingest.models import DLQEvent

    dlq = DLQEvent.model_validate_json(value)
    assert dlq.original_key == "binance#solusdt"
    assert dlq.error_type == "ValueError"
