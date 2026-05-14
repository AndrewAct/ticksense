from pydantic import field_validator
from pydantic_settings import BaseSettings


class Settings(BaseSettings):
    kafka_bootstrap_servers: str = "localhost:19092"
    kafka_security_protocol: str = "PLAINTEXT"
    binance_ws_base_url: str = "wss://stream.binance.com:9443/ws"
    binance_rest_base_url: str = "https://api.binance.com"
    ingest_symbols: list[str] = ["btcusdt", "ethusdt"]
    log_level: str = "INFO"
    env: str = "local"

    model_config = {"env_file": ".env", "frozen": True}

    @field_validator("ingest_symbols", mode="before")
    @classmethod
    def parse_symbols(cls, v: object) -> list[str]:
        if isinstance(v, str):
            return [s.strip().lower() for s in v.split(",") if s.strip()]
        return v  # type: ignore[return-value]


settings = Settings()
