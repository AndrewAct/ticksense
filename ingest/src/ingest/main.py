import asyncio
import logging

import structlog

from .client import BinanceOrderBookClient
from .producer import KafkaOrderBookProducer
from .settings import settings


def _configure_logging(log_level: str) -> None:
    logging.basicConfig(
        level=getattr(logging, log_level.upper(), logging.INFO),
        format="%(message)s",
    )
    structlog.configure(
        processors=[
            structlog.contextvars.merge_contextvars,
            structlog.stdlib.add_log_level,
            structlog.processors.TimeStamper(fmt="iso"),
            structlog.dev.ConsoleRenderer(),
        ],
        wrapper_class=structlog.stdlib.BoundLogger,
        context_class=dict,
        logger_factory=structlog.stdlib.LoggerFactory(),
        cache_logger_on_first_use=True,
    )


async def _run() -> None:
    log = structlog.get_logger()
    _configure_logging(settings.log_level)

    log.info("ingest_starting", symbols=settings.ingest_symbols)
    producer = KafkaOrderBookProducer(settings)
    await producer.start()

    try:
        clients = [
            BinanceOrderBookClient(symbol=sym, producer=producer, settings=settings)
            for sym in settings.ingest_symbols
        ]
        await asyncio.gather(*[c.run() for c in clients])
    except asyncio.CancelledError:
        log.info("ingest_stopping")
    finally:
        await producer.stop()
        log.info("ingest_stopped")


def main() -> None:
    try:
        asyncio.run(_run())
    except KeyboardInterrupt:
        pass


if __name__ == "__main__":
    main()
