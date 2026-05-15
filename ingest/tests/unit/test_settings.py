import pytest

from ingest.settings import Settings


class TestSettings:
    def test_defaults(self) -> None:
        s = Settings()
        assert s.kafka_bootstrap_servers == "localhost:19092"
        assert s.log_level == "INFO"
        assert "btcusdt" in s.ingest_symbols

    def test_symbols_from_list(self) -> None:
        s = Settings(ingest_symbols=["btcusdt", "ethusdt", "solusdt"])
        assert s.ingest_symbols == ["btcusdt", "ethusdt", "solusdt"]

    def test_symbols_single_item(self) -> None:
        s = Settings(ingest_symbols=["btcusdt"])
        assert s.ingest_symbols == ["btcusdt"]

    def test_symbols_preserves_case_as_provided(self) -> None:
        s = Settings(ingest_symbols=["btcusdt", "ethusdt"])
        assert "btcusdt" in s.ingest_symbols

    def test_override_bootstrap_servers(self) -> None:
        s = Settings(kafka_bootstrap_servers="kafka:9092")
        assert s.kafka_bootstrap_servers == "kafka:9092"

    def test_frozen(self) -> None:
        s = Settings()
        with pytest.raises((TypeError, ValueError)):
            s.log_level = "DEBUG"  # type: ignore[misc]
