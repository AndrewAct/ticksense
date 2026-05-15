from pydantic_settings import BaseSettings


class Settings(BaseSettings):
    kafka_bootstrap_servers: str = "localhost:19092"
    kafka_security_protocol: str = "PLAINTEXT"
    binance_ws_base_url: str = "wss://stream.binance.us:9443/ws"
    binance_rest_base_url: str = "https://api.binance.us"
    ingest_symbols: list[str] = ["btcusdt", "ethusdt"]
    log_level: str = "INFO"
    env: str = "local"

    model_config = {"env_file": ".env", "frozen": True, "extra": "ignore"}


settings = Settings()
