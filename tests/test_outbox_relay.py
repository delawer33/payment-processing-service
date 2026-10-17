"""Relay outbox: публикация пачки, откат при ошибке, пропуск блокировок и неизвестных типов."""

import asyncio
import json
import uuid

import pytest
from prometheus_client import REGISTRY
from sqlalchemy import select, text
from sqlalchemy.ext.asyncio import AsyncEngine, AsyncSession, async_sessionmaker

from payment_processing.consumer.metrics import OUTBOX_OLDEST_AGE
from payment_processing.outbox import relay
from payment_processing.outbox.models import OutboxEvent
from payment_processing.outbox.relay import relay_once

BATCH = 100


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
        assert await relay_once(session, publisher, batch_size=BATCH) == 2

    assert [(json.loads(b), rk) for b, rk, _ in publisher.sent] == [
        ({"payment_id": ids[0]}, "payments.new"),
        ({"payment_id": ids[1]}, "payments.new"),
    ]
    assert [m for _, _, m in publisher.sent] == ["1", "2"]
    assert await _published_flags(sessionmaker) == [True, True]

    async with sessionmaker() as session:
        assert await relay_once(session, publisher, batch_size=BATCH) == 0
    assert len(publisher.sent) == 2


async def test_relay_marks_sent_only_after_publish_succeeds(
    sessionmaker: async_sessionmaker[AsyncSession],
) -> None:
    """Ошибка на втором событии откатывает пачку: первое уйдёт повторно (at-least-once)."""
    await _add_events(sessionmaker, 2)
    failing = FakePublisher(fail_on_call=2)

    async with sessionmaker() as session:
        with pytest.raises(ConnectionError):
            await relay_once(session, failing, batch_size=BATCH)

    assert len(failing.sent) == 1
    assert await _published_flags(sessionmaker) == [False, False]

    healthy = FakePublisher()
    async with sessionmaker() as session:
        assert await relay_once(session, healthy, batch_size=BATCH) == 2
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
            assert await relay_once(other, publisher, batch_size=BATCH) == 0

    assert publisher.sent == []
    assert await _published_flags(sessionmaker) == [False]


async def test_relay_skips_unknown_event_type_and_publishes_the_rest(
    sessionmaker: async_sessionmaker[AsyncSession],
    caplog: pytest.LogCaptureFixture,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    # fileConfig в migrations/env.py отключает уже созданные логгеры при программном upgrade.
    monkeypatch.setattr(relay.logger, "disabled", False)
    await _add_events(sessionmaker, 1)
    async with sessionmaker() as session:
        session.add(
            OutboxEvent(aggregate_id=uuid.uuid4(), event_type="payment.unknown", payload={})
        )
        await session.commit()
    await _add_events(sessionmaker, 1)
    publisher = FakePublisher()

    async with sessionmaker() as session:
        assert await relay_once(session, publisher, batch_size=BATCH) == 2

    assert [m for _, _, m in publisher.sent] == ["1", "3"]
    assert await _published_flags(sessionmaker) == [True, False, True]
    assert "payment.unknown" in caplog.text


async def test_relay_counts_published_events(
    sessionmaker: async_sessionmaker[AsyncSession],
) -> None:
    before = REGISTRY.get_sample_value("outbox_published_total") or 0.0
    await _add_events(sessionmaker, 3)

    async with sessionmaker() as session:
        await relay_once(session, FakePublisher(), batch_size=BATCH)

    assert REGISTRY.get_sample_value("outbox_published_total") == before + 3


async def test_oldest_unpublished_age_reflects_waiting_event(
    sessionmaker: async_sessionmaker[AsyncSession],
) -> None:
    await _add_events(sessionmaker, 1)
    async with sessionmaker() as session:
        await session.execute(text("UPDATE outbox SET created_at = now() - interval '30 seconds'"))
        await session.commit()

    async with sessionmaker() as session:
        await relay.refresh_oldest_unpublished_age(session)

    age = REGISTRY.get_sample_value("outbox_oldest_unpublished_age_seconds")
    assert age is not None
    assert 30 <= age < 60


async def test_oldest_unpublished_age_is_zero_for_empty_outbox(
    sessionmaker: async_sessionmaker[AsyncSession],
) -> None:
    async with sessionmaker() as session:
        await relay.refresh_oldest_unpublished_age(session)

    assert REGISTRY.get_sample_value("outbox_oldest_unpublished_age_seconds") == 0


async def test_oldest_unpublished_age_ignores_events_of_unknown_type(
    sessionmaker: async_sessionmaker[AsyncSession],
) -> None:
    """Событие неизвестного типа ждёт ручного разбора и не должно выглядеть как отставание relay."""
    async with sessionmaker() as session:
        session.add(
            OutboxEvent(aggregate_id=uuid.uuid4(), event_type="payment.unknown", payload={})
        )
        await session.commit()
        await session.execute(text("UPDATE outbox SET created_at = now() - interval '1 hour'"))
        await session.commit()

    async with sessionmaker() as session:
        await relay.refresh_oldest_unpublished_age(session)

    assert REGISTRY.get_sample_value("outbox_oldest_unpublished_age_seconds") == 0


async def test_relay_loop_refreshes_age_even_when_broker_is_down(
    sessionmaker: async_sessionmaker[AsyncSession], monkeypatch: pytest.MonkeyPatch
) -> None:
    await _add_events(sessionmaker, 1)
    async with sessionmaker() as session:
        await session.execute(text("UPDATE outbox SET created_at = now() - interval '30 seconds'"))
        await session.commit()
    OUTBOX_OLDEST_AGE.set(0)

    async def stop_after_first_pass(delay: float) -> None:
        raise asyncio.CancelledError

    monkeypatch.setattr(asyncio, "sleep", stop_after_first_pass)
    with pytest.raises(asyncio.CancelledError):
        await relay.run_relay(sessionmaker, FakePublisher(fail_on_call=1), interval=0.5)

    age = REGISTRY.get_sample_value("outbox_oldest_unpublished_age_seconds")
    assert age is not None
    assert age >= 30
