from pydantic_settings import BaseSettings, SettingsConfigDict


class Settings(BaseSettings):
    trino_host: str = "localhost"
    trino_port: int = 8082
    trino_catalog: str = "iceberg"
    trino_user: str = "api"
    log_level: str = "INFO"

    model_config = SettingsConfigDict(env_file=".env", frozen=True, extra="ignore")


settings = Settings()
