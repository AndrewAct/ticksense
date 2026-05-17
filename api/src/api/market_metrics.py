from prometheus_client import Gauge

MID_PRICE = Gauge(
    "market_mid_price_usd",
    "Current mid price in USD",
    ["symbol", "exchange"],
)

SPREAD_BPS = Gauge(
    "market_spread_bps",
    "Bid-ask spread in basis points",
    ["symbol", "exchange"],
)

IMBALANCE = Gauge(
    "market_bid_ask_imbalance",
    "Order book imbalance (-1 = full sell pressure, +1 = full buy pressure)",
    ["symbol", "exchange"],
)

STALENESS = Gauge(
    "market_staleness_seconds",
    "Seconds since last market event was ingested",
    ["symbol", "exchange"],
)

HEALTH_SCORE = Gauge(
    "pipeline_health_score",
    "Pipeline health score (0 = dead, 1 = healthy)",
    ["symbol", "exchange"],
)
