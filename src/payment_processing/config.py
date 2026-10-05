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


@lru_cache
def get_settings() -> Settings:
    return Settings()
