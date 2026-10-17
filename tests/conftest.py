"""Общие фикстуры: Postgres в testcontainers, миграции через Alembic, приложение по ASGI."""

import os
from collections.abc import AsyncIterator, Iterator

import pytest
from alembic import command
from alembic.config import Config
from fastapi import FastAPI
from httpx import ASGITransport, AsyncClient
from redis.asyncio import Redis
from sqlalchemy import text
from sqlalchemy.ext.asyncio import AsyncEngine
from testcontainers.community.postgres import PostgresContainer
from testcontainers.community.redis import RedisContainer

from payment_processing.config import get_settings
from payment_processing.db import get_engine, get_sessionmaker
from payment_processing.main import create_app
from payment_processing.ratelimit import get_redis

API_KEY = "test-api-key"


def _clear_caches() -> None:
    get_settings.cache_clear()
    get_engine.cache_clear()
    get_sessionmaker.cache_clear()
    get_redis.cache_clear()


@pytest.fixture(scope="session")
def redis_url() -> Iterator[str]:
    with RedisContainer("redis:7-alpine") as rd:
        yield f"redis://{rd.get_container_host_ip()}:{rd.get_exposed_port(6379)}/0"


@pytest.fixture(scope="session")
def database_url(redis_url: str) -> Iterator[str]:
    with PostgresContainer("postgres:16-alpine", driver="asyncpg") as pg:
        url = pg.get_connection_url()
        os.environ["DATABASE_URL"] = url
        os.environ["REDIS_URL"] = redis_url
        os.environ["RABBITMQ_URL"] = "amqp://guest:guest@localhost:5672/"
        os.environ["API_KEY"] = API_KEY
        _clear_caches()
        command.upgrade(Config("alembic.ini"), "head")
        yield url
        _clear_caches()


@pytest.fixture
async def engine(database_url: str) -> AsyncIterator[AsyncEngine]:
    _clear_caches()
    eng = get_engine()
    async with eng.begin() as conn:
        await conn.execute(text("TRUNCATE payments, outbox RESTART IDENTITY"))
    yield eng
    await eng.dispose()
    _clear_caches()


@pytest.fixture
async def redis(engine: AsyncEngine) -> AsyncIterator[Redis]:
    client = get_redis()
    await client.flushdb()
    yield client
    await client.aclose()


@pytest.fixture
def app(redis: Redis) -> FastAPI:
    return create_app()


@pytest.fixture
async def client(app: FastAPI) -> AsyncIterator[AsyncClient]:
    async with AsyncClient(
        transport=ASGITransport(app=app),
        base_url="http://test",
        headers={"X-API-Key": API_KEY},
    ) as c:
        yield c
