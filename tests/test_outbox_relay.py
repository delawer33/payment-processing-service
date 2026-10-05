"""Relay outbox: публикация пачки, откат при ошибке, пропуск чужих блокировок."""

import json
import uuid

import pytest
from sqlalchemy import select, text
from sqlalchemy.ext.asyncio import AsyncEngine, AsyncSession, async_sessionmaker

from payment_processing.outbox.models import OutboxEvent
from payment_processing.outbox.relay import relay_once


class FakePublisher:
    def __init__(self, fail_on_call: int | None = None) -> None:
        self.sent: list[tuple[bytes, str, str]] = []
        self._calls = 0
        self._fail_on_call = fail_on_call

    async def publish(self, body: bytes, routing_key: str, *, message_id: str) -> None:
        self._calls += 1
        if self._calls == self._fail_on_call:
            raise ConnectionError("broker is down")
        self.sent.append((body, routing_key, message_id))


@pytest.fixture
def sessionmaker(engine: AsyncEngine) -> async_sessionmaker[AsyncSession]:
    return async_sessionmaker(engine, expire_on_commit=False)


async def _add_events(sessionmaker: async_sessionmaker[AsyncSession], count: int) -> list[str]:
    ids = [str(uuid.uuid4()) for _ in range(count)]
    async with sessionmaker() as session:
        session.add_all(
            OutboxEvent(
                aggregate_id=uuid.UUID(pid),
                event_type="payment.created",
                payload={"payment_id": pid},
            )
            for pid in ids
        )
        await session.commit()
    return ids


async def _published_flags(sessionmaker: async_sessionmaker[AsyncSession]) -> list[bool]:
    async with sessionmaker() as session:
        rows = await session.scalars(select(OutboxEvent).order_by(OutboxEvent.id))
        return [e.published_at is not None for e in rows]


async def test_relay_publishes_unpublished_events_and_marks_them(
    sessionmaker: async_sessionmaker[AsyncSession],
) -> None:
    ids = await _add_events(sessionmaker, 2)
    publisher = FakePublisher()

    async with sessionmaker() as session:
        assert await relay_once(session, publisher) == 2

    assert [(json.loads(b), rk) for b, rk, _ in publisher.sent] == [
        ({"payment_id": ids[0]}, "payments.new"),
        ({"payment_id": ids[1]}, "payments.new"),
    ]
    assert [m for _, _, m in publisher.sent] == ["1", "2"]
    assert await _published_flags(sessionmaker) == [True, True]

    async with sessionmaker() as session:
        assert await relay_once(session, publisher) == 0
    assert len(publisher.sent) == 2


async def test_relay_marks_sent_only_after_publish_succeeds(
    sessionmaker: async_sessionmaker[AsyncSession],
) -> None:
    """Ошибка на втором событии откатывает пачку: первое уйдёт повторно (at-least-once)."""
    await _add_events(sessionmaker, 2)
    failing = FakePublisher(fail_on_call=2)

    async with sessionmaker() as session:
        with pytest.raises(ConnectionError):
            await relay_once(session, failing)

    assert len(failing.sent) == 1
    assert await _published_flags(sessionmaker) == [False, False]

    healthy = FakePublisher()
    async with sessionmaker() as session:
        assert await relay_once(session, healthy) == 2
    assert [m for _, _, m in healthy.sent] == ["1", "2"]
    assert await _published_flags(sessionmaker) == [True, True]


async def test_relay_skips_rows_locked_by_another_relay(
    sessionmaker: async_sessionmaker[AsyncSession],
) -> None:
    await _add_events(sessionmaker, 1)
    publisher = FakePublisher()

    async with sessionmaker() as holder:
        await holder.execute(text("SELECT id FROM outbox FOR UPDATE"))
        async with sessionmaker() as other:
            assert await relay_once(other, publisher) == 0

    assert publisher.sent == []
    assert await _published_flags(sessionmaker) == [False]
