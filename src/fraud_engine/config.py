from functools import lru_cache

from pydantic import SecretStr
from pydantic_settings import BaseSettings, SettingsConfigDict


class Settings(BaseSettings):
    model_config = SettingsConfigDict(env_file=".env", extra="ignore")

    database_url: str = "postgresql+psycopg://fraud:fraud@127.0.0.1:5433/fraud"
    redis_url: str = "redis://127.0.0.1:6379/0"
    kafka_bootstrap_servers: str = "127.0.0.1:9092"
    kafka_raw_topic: str = "transactions.raw"
    kafka_dlq_topic: str = "fraud.dead-letter"
    model_artifact_dir: str = "artifacts/fraud-v2"
    fraud_review_api_key: SecretStr | None = None
    inference_timeout_seconds: float = 2.0
    retry_initial_seconds: float = 0.25
    retry_max_seconds: float = 10.0


@lru_cache
def get_settings() -> Settings:
    return Settings()
