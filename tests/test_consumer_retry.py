"""Сообщение уровня брокера: повтор через payments.retry и DLQ после трёх попыток (ADR 0004)."""

import importlib
import uuid
from collections.abc import AsyncIterator, Mapping
from types import ModuleType
from typing import TYPE_CHECKING, Any
from unittest.mock import AsyncMock, MagicMock

import pytest
from faststream.rabbit import RabbitBroker, RabbitMessage, TestRabbitBroker

from payment_processing.messaging.topology import EXCHANGE, NEW_QUEUE, RETRY_QUEUE

if TYPE_CHECKING:
    from aio_pika.abc import FieldValue


@pytest.fixture
def consumer_app(database_url: str) -> ModuleType:
    # Модуль читает настройки при импорте, поэтому импорт только после фикстуры окружения.
    return importlib.import_module("payment_processing.consumer.app")


@pytest.fixture
async def test_broker(
    consumer_app: ModuleType, monkeypatch: pytest.MonkeyPatch
) -> AsyncIterator[RabbitBroker]:
    monkeypatch.setattr(
        consumer_app,
        "_deps",
        consumer_app.Dependencies(sessionmaker=MagicMock(), gateway=MagicMock(), http=MagicMock()),
    )
    async with TestRabbitBroker(consumer_app.broker) as broker:
        yield broker


@pytest.fixture
def republish(consumer_app: ModuleType, monkeypatch: pytest.MonkeyPatch) -> AsyncMock:
    """Перехватывает публикации в payments.retry; остальные (входящее сообщение теста) пропускает."""
    original = consumer_app.broker.publish

    async def publish(*args: Any, **kwargs: Any) -> Any:
        if kwargs.get("queue") == RETRY_QUEUE:
            return None  # у retry читателей нет, тестовый брокер без подписчика упал бы
        return await original(*args, **kwargs)

    mock = AsyncMock(side_effect=publish)
    monkeypatch.setattr(consumer_app.broker, "publish", mock)
    return mock


@pytest.fixture
def spy_message(monkeypatch: pytest.MonkeyPatch) -> dict[str, AsyncMock]:
    spies = {"ack": AsyncMock(), "reject": AsyncMock()}
    monkeypatch.setattr(RabbitMessage, "ack", lambda self, *a, **kw: spies["ack"](*a, **kw))
    monkeypatch.setattr(RabbitMessage, "reject", lambda self, *a, **kw: spies["reject"](*a, **kw))
    return spies


def _retry_publishes(publish: AsyncMock) -> list[Mapping[str, Any]]:
    """Публикации в payments.retry; входящее сообщение теста идёт тем же `publish`."""
    return [c.kwargs for c in publish.await_args_list if c.kwargs.get("queue") == RETRY_QUEUE]


async def _deliver(test_broker: RabbitBroker, attempt: int | None) -> None:
    headers: dict[str, FieldValue] | None = None if attempt is None else {"x-attempt": attempt}
    await test_broker.publish(
        {"payment_id": str(uuid.uuid4())}, queue=NEW_QUEUE, exchange=EXCHANGE, headers=headers
    )


@pytest.mark.parametrize(
    ("attempt", "expiration", "next_attempt"),
    [(None, 1.0, 1), (0, 1.0, 1), (1, 2.0, 2)],
)
async def test_failure_republishes_to_retry_with_backoff_and_incremented_attempt(
    test_broker: RabbitBroker,
    consumer_app: ModuleType,
    republish: AsyncMock,
    spy_message: dict[str, AsyncMock],
    monkeypatch: pytest.MonkeyPatch,
    attempt: int | None,
    expiration: float,
    next_attempt: int,
) -> None:
    monkeypatch.setattr(consumer_app, "process_payment", AsyncMock(side_effect=RuntimeError))

    await _deliver(test_broker, attempt)

    (kwargs,) = _retry_publishes(republish)
    assert kwargs["headers"] == {"x-attempt": next_attempt}
    # Секунды: aio-pika переводит 1.0 в TTL 1000 мс на проводе.
    assert kwargs["expiration"] == expiration
    spy_message["ack"].assert_awaited_once()
    spy_message["reject"].assert_not_awaited()


async def test_third_failure_rejects_to_dlq(
    test_broker: RabbitBroker,
    consumer_app: ModuleType,
    republish: AsyncMock,
    spy_message: dict[str, AsyncMock],
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setattr(consumer_app, "process_payment", AsyncMock(side_effect=RuntimeError))

    await _deliver(test_broker, 2)

    spy_message["reject"].assert_awaited_once_with(requeue=False)
    spy_message["ack"].assert_not_awaited()
    assert _retry_publishes(republish) == []


async def test_success_acks_without_republish(
    test_broker: RabbitBroker,
    consumer_app: ModuleType,
    republish: AsyncMock,
    spy_message: dict[str, AsyncMock],
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setattr(consumer_app, "process_payment", AsyncMock())

    await _deliver(test_broker, None)

    spy_message["ack"].assert_awaited_once()
    spy_message["reject"].assert_not_awaited()
    assert _retry_publishes(republish) == []
