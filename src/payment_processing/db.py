"""Database access: one engine per process, one session per HTTP request.

The unit of work is the request. `get_session` commits on success and rolls back on any
exception; services receive the session as an argument and never commit themselves.
Schema changes go through Alembic (`migrations/`), never `Base.metadata.create_all`.
"""

from collections.abc import AsyncIterator
from functools import lru_cache

from sqlalchemy.ext.asyncio import (
    AsyncEngine,
    AsyncSession,
    async_sessionmaker,
    create_async_engine,
)
from sqlalchemy.orm import DeclarativeBase

from payment_processing.config import get_settings


class Base(DeclarativeBase):
    """Declarative base every model inherits from; Alembic reads `Base.metadata`."""


@lru_cache
def get_engine() -> AsyncEngine:
    return create_async_engine(get_settings().database_url)


@lru_cache
def get_sessionmaker() -> async_sessionmaker[AsyncSession]:
    return async_sessionmaker(get_engine(), expire_on_commit=False)


async def get_session() -> AsyncIterator[AsyncSession]:
    async with get_sessionmaker()() as session:
        try:
            yield session
            await session.commit()
        except Exception:
            await session.rollback()
            raise
