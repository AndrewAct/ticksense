import asyncio

import structlog

from .market_metrics import HEALTH_SCORE, IMBALANCE, MID_PRICE, SPREAD_BPS, STALENESS
from .trino_client import TrinoClient

log = structlog.get_logger()

POLL_INTERVAL = 30

_LIQUIDITY_SQL = """
SELECT exchange, symbol, mid_price, spread_bps,
       COALESCE(imbalance, 0.0) AS imbalance,
       staleness_seconds
FROM iceberg.marts.mart_liquidity
"""

_HEALTH_SQL = """
SELECT exchange, symbol, health_score
FROM iceberg.marts.mart_exchange_health
"""


async def _poll_once(client: TrinoClient) -> None:
    try:
        rows = await client.fetch(_LIQUIDITY_SQL)
        for r in rows:
            lbl = {"symbol": r["symbol"], "exchange": r["exchange"]}
            MID_PRICE.labels(**lbl).set(r["mid_price"])
            SPREAD_BPS.labels(**lbl).set(r["spread_bps"])
            IMBALANCE.labels(**lbl).set(r["imbalance"])
            STALENESS.labels(**lbl).set(r["staleness_seconds"])
        log.info("market_poll_ok", symbols=len(rows))
    except Exception as exc:
        log.warning("market_poll_liquidity_failed", error=str(exc))

    try:
        rows = await client.fetch(_HEALTH_SQL)
        for r in rows:
            HEALTH_SCORE.labels(symbol=r["symbol"], exchange=r["exchange"]).set(r["health_score"])
    except Exception as exc:
        log.warning("market_poll_health_failed", error=str(exc))


async def run_poller(client: TrinoClient) -> None:
    while True:
        await _poll_once(client)
        await asyncio.sleep(POLL_INTERVAL)
