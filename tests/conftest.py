"""Общие фикстуры: Postgres в testcontainers, миграции через Alembic, приложение по ASGI."""

import os
from collections.abc import AsyncIterator, Iterator

import pytest
from alembic import command
from alembic.config import Config
from fastapi import FastAPI
from httpx import ASGITransport, AsyncClient
from sqlalchemy import text
from sqlalchemy.ext.asyncio import AsyncEngine
from testcontainers.community.postgres import PostgresContainer

from payment_processing.config import get_settings
from payment_processing.db import get_engine, get_sessionmaker
from payment_processing.main import create_app

API_KEY = "test-api-key"


def _clear_caches() -> None:
    get_settings.cache_clear()
    get_engine.cache_clear()
    get_sessionmaker.cache_clear()


@pytest.fixture(scope="session")
def database_url() -> Iterator[str]:
    with PostgresContainer("postgres:16-alpine", driver="asyncpg") as pg:
        url = pg.get_connection_url()
        os.environ["DATABASE_URL"] = url
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
def app(engine: AsyncEngine) -> FastAPI:
    return create_app()


@pytest.fixture
async def client(app: FastAPI) -> AsyncIterator[AsyncClient]:
    async with AsyncClient(
        transport=ASGITransport(app=app),
        base_url="http://test",
        headers={"X-API-Key": API_KEY},
    ) as c:
        yield c
