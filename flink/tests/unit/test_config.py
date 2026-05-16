"""Unit tests for flink/jobs/lib/config.py."""

from __future__ import annotations

from lib.config import Config


class TestConfig:
    def test_defaults(self):
        cfg = Config()
        assert cfg.kafka_brokers == "redpanda:9092"
        assert cfg.iceberg_uri == "http://iceberg-rest:8181"
        assert cfg.checkpoint_interval_ms == 60000

    def test_env_override(self, monkeypatch):
        monkeypatch.setenv("KAFKA_BOOTSTRAP_SERVERS", "broker:9092")
        monkeypatch.setenv("ICEBERG_REST_URI", "http://catalog:8181")
        monkeypatch.setenv("CHECKPOINT_INTERVAL_MS", "30000")
        cfg = Config()
        assert cfg.kafka_brokers == "broker:9092"
        assert cfg.iceberg_uri == "http://catalog:8181"
        assert cfg.checkpoint_interval_ms == 30000

    def test_topic_constants(self):
        assert Config.TOPIC_RAW == "market.raw.orderbook"
        assert Config.TOPIC_BOOK_TICKER == "market.normalized.book_ticker"
        assert Config.TOPIC_DLQ == "market.dlq"
        assert Config.TOPIC_CDC == "postgres.public.symbol_config"

    def test_group_constants(self):
        assert Config.GROUP_NORMALIZE == "flink-normalize"
        assert Config.GROUP_OHLCV == "flink-ohlcv-1m"
        assert Config.GROUP_CDC_BRONZE == "flink-cdc-bronze"
        assert Config.GROUP_CDC_NORMALIZED == "flink-cdc-normalized"

    def test_as_dict_contains_required_keys(self):
        cfg = Config()
        d = cfg.as_dict()
        required = {
            "kafka_brokers",
            "iceberg_uri",
            "iceberg_warehouse",
            "s3_endpoint",
            "s3_region",
            "topic_raw",
            "topic_book_ticker",
            "topic_dlq",
            "topic_cdc",
            "group_normalize",
            "group_ohlcv",
            "group_cdc_bronze",
            "group_cdc_normalized",
        }
        assert required <= d.keys()

    def test_as_dict_values_are_strings(self):
        cfg = Config()
        for k, v in cfg.as_dict().items():
            assert isinstance(v, str), f"Key {k!r} has non-string value {v!r}"

    def test_state_ttl_ms(self):
        assert Config.STATE_TTL_MS == 3_600_000

    def test_late_threshold_seconds(self):
        assert Config.LATE_THRESHOLD_SECONDS == 30
