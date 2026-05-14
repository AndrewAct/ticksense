import pytest

from ingest.settings import Settings


class TestSettings:
    def test_defaults(self) -> None:
        s = Settings()
        assert s.kafka_bootstrap_servers == "localhost:19092"
        assert s.log_level == "INFO"
        assert "btcusdt" in s.ingest_symbols

    def test_symbols_from_comma_string(self) -> None:
        s = Settings(ingest_symbols="btcusdt,ethusdt, solusdt")  # type: ignore[arg-type]
        assert s.ingest_symbols == ["btcusdt", "ethusdt", "solusdt"]

    def test_symbols_whitespace_stripped(self) -> None:
        s = Settings(ingest_symbols="  BTCUSDT ,  ETHUSDT  ")  # type: ignore[arg-type]
        assert s.ingest_symbols == ["btcusdt", "ethusdt"]

    def test_symbols_empty_entries_skipped(self) -> None:
        s = Settings(ingest_symbols="btcusdt,,ethusdt,")  # type: ignore[arg-type]
        assert s.ingest_symbols == ["btcusdt", "ethusdt"]

    def test_override_bootstrap_servers(self) -> None:
        s = Settings(kafka_bootstrap_servers="kafka:9092")
        assert s.kafka_bootstrap_servers == "kafka:9092"

    def test_frozen(self) -> None:
        s = Settings()
        with pytest.raises((TypeError, ValueError)):
            s.log_level = "DEBUG"  # type: ignore[misc]
