"""Runtime configuration: everything comes from the environment (12-factor).

`.env` is read for local runs; in containers the variables arrive from compose.
Business code never branches on the environment name.
"""

from functools import lru_cache
from typing import Literal

from pydantic_settings import BaseSettings, SettingsConfigDict


class Settings(BaseSettings):
    model_config = SettingsConfigDict(env_file=".env", extra="ignore")

    environment: Literal["local", "test", "prod"] = "local"
    log_level: str = "INFO"
    # No default on purpose: a missing URL must fail loudly, not fall back to a local guess.
    database_url: str
    rabbitmq_url: str
    outbox_poll_interval: float = 0.5
    outbox_batch_size: int = 100
    # Static key clients send in `X-API-Key`; no default so an unset key cannot open the API.
    api_key: str
    gateway_delay_min: float = 2.0
    gateway_delay_max: float = 5.0
    gateway_success_rate: float = 0.9
    # Ключ подписи webhook (`X-Signature`); дефолт годится только для локального запуска.
    webhook_secret: str = "change-me"
    webhook_timeout: float = 5.0
    # Задержка перед первым повтором, секунды; каждая следующая вдвое больше (ADR 0004).
    retry_base_delay: float = 1.0
    # Сколько сообщений consumer держит в работе одновременно; каждое занимает соединение с БД.
    consumer_prefetch: int = 10


@lru_cache
def get_settings() -> Settings:
    return Settings()
