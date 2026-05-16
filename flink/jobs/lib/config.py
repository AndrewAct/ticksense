"""Flink job settings resolved from environment variables at import time."""

from __future__ import annotations

import os


class Config:
    def __init__(self) -> None:
        self.kafka_brokers: str = os.environ.get("KAFKA_BOOTSTRAP_SERVERS", "redpanda:9092")
        self.iceberg_uri: str = os.environ.get("ICEBERG_REST_URI", "http://iceberg-rest:8181")
        self.iceberg_warehouse: str = os.environ.get(
            "ICEBERG_WAREHOUSE", "s3://ticksense/warehouse"
        )
        self.s3_endpoint: str = os.environ.get("S3_ENDPOINT", "http://minio:9000")
        self.s3_region: str = os.environ.get("S3_REGION", "us-east-1")
        self.checkpoint_interval_ms: int = int(os.environ.get("CHECKPOINT_INTERVAL_MS", "60000"))

    TOPIC_RAW = "market.raw.orderbook"
    TOPIC_BOOK_TICKER = "market.normalized.book_ticker"
    TOPIC_DLQ = "market.dlq"

    TOPIC_CDC = "postgres.public.symbol_config"

    GROUP_NORMALIZE = "flink-normalize"
    GROUP_OHLCV = "flink-ohlcv-1m"
    GROUP_CDC_BRONZE = "flink-cdc-bronze"
    GROUP_CDC_NORMALIZED = "flink-cdc-normalized"

    # Events whose exchange_event_ts lags wall-clock by more than this → DLQ
    LATE_THRESHOLD_SECONDS: int = 30
    # Table-API state TTL applied to dedup ROW_NUMBER windows
    STATE_TTL_MS: int = 3_600_000  # 1 hour

    def as_dict(self) -> dict[str, str]:
        """Return all substitution keys for .sql template files."""
        return {
            "kafka_brokers": self.kafka_brokers,
            "iceberg_uri": self.iceberg_uri,
            "iceberg_warehouse": self.iceberg_warehouse,
            "s3_endpoint": self.s3_endpoint,
            "s3_region": self.s3_region,
            "topic_raw": self.TOPIC_RAW,
            "topic_book_ticker": self.TOPIC_BOOK_TICKER,
            "topic_dlq": self.TOPIC_DLQ,
            "topic_cdc": self.TOPIC_CDC,
            "group_normalize": self.GROUP_NORMALIZE,
            "group_ohlcv": self.GROUP_OHLCV,
            "group_cdc_bronze": self.GROUP_CDC_BRONZE,
            "group_cdc_normalized": self.GROUP_CDC_NORMALIZED,
        }
