from datetime import UTC, datetime

import structlog
from aiokafka import AIOKafkaProducer

from .models import DLQEvent, OrderBookEvent
from .settings import Settings

log = structlog.get_logger()

TOPIC_RAW_ORDERBOOK = "market.raw.orderbook"
TOPIC_DLQ = "market.dlq"


class KafkaOrderBookProducer:
    def __init__(self, settings: Settings) -> None:
        self._settings = settings
        self._producer: AIOKafkaProducer | None = None

    async def start(self) -> None:
        self._producer = AIOKafkaProducer(
            bootstrap_servers=self._settings.kafka_bootstrap_servers,
            security_protocol=self._settings.kafka_security_protocol,
            enable_idempotence=True,
            acks="all",
            request_timeout_ms=30_000,
            retry_backoff_ms=250,
        )
        await self._producer.start()
        log.info("kafka_producer_started", servers=self._settings.kafka_bootstrap_servers)

    async def stop(self) -> None:
        if self._producer is not None:
            await self._producer.flush()
            await self._producer.stop()
            log.info("kafka_producer_stopped")

    async def produce(self, event: OrderBookEvent) -> None:
        assert self._producer is not None, "call start() before produce()"
        key = f"binance#{event.symbol}".encode()

        try:
            value = event.model_dump_json().encode()
        except Exception as exc:
            log.error("serialization_error", event_id=event.event_id, error=str(exc))
            await self._send_to_dlq(key=key.decode(), payload=repr(event), error=exc)
            return

        record = await self._producer.send_and_wait(TOPIC_RAW_ORDERBOOK, value=value, key=key)
        log.info(
            "event_produced",
            topic=record.topic,
            partition=record.partition,
            offset=record.offset,
            event_id=event.event_id,
        )

    async def _send_to_dlq(self, key: str, payload: str, error: Exception) -> None:
        if self._producer is None:
            return
        dlq = DLQEvent(
            original_key=key,
            original_payload=payload,
            error_type=type(error).__name__,
            error_message=str(error),
            ingest_ts=datetime.now(UTC),
        )
        try:
            await self._producer.send_and_wait(
                TOPIC_DLQ,
                value=dlq.model_dump_json().encode(),
                key=key.encode(),
            )
            log.warning("event_routed_to_dlq", original_key=key, error_type=dlq.error_type)
        except Exception as dlq_exc:
            log.error("dlq_send_failed", original_key=key, error=str(dlq_exc))
