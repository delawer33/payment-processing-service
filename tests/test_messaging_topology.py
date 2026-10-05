"""Топология RabbitMQ (ADR 0004): аргументы очередей и доставка подписчику."""

import asyncio
import uuid
from unittest.mock import AsyncMock, MagicMock

import pytest
from faststream.rabbit import RabbitQueue, TestRabbitBroker

from payment_processing.messaging.schemas import PaymentCreated
from payment_processing.messaging.topology import (
    ATTEMPT_HEADER,
    DLQ_QUEUE,
    EXCHANGE,
    MAX_ATTEMPTS,
    NEW_QUEUE,
    RETRY_QUEUE,
)


@pytest.mark.usefixtures("database_url")
async def test_topology_declares_three_queues_with_dlx_arguments(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    from payment_processing.consumer.app import broker, handle_payment_created

    monkeypatch.setattr("payment_processing.consumer.app.process_payment", AsyncMock())
    monkeypatch.setattr("payment_processing.consumer.app._deps", MagicMock(), raising=False)
    assert EXCHANGE.name == "payments"
    assert (
        NEW_QUEUE.arguments.items()
        >= {
            "x-dead-letter-exchange": "payments",
            "x-dead-letter-routing-key": "payments.dlq",
        }.items()
    )
    assert (
        RETRY_QUEUE.arguments.items()
        >= {
            "x-dead-letter-exchange": "payments",
            "x-dead-letter-routing-key": "payments.new",
        }.items()
    )
    assert "x-dead-letter-exchange" not in DLQ_QUEUE.arguments
    assert all(q.durable for q in (NEW_QUEUE, RETRY_QUEUE, DLQ_QUEUE))
    assert (ATTEMPT_HEADER, MAX_ATTEMPTS) == ("x-attempt", 3)

    payment_id = uuid.uuid4()
    async with TestRabbitBroker(broker) as test_broker:
        await test_broker.publish(
            {"payment_id": str(payment_id)}, queue=NEW_QUEUE, exchange=EXCHANGE
        )
        handle_payment_created.mock.assert_called_once_with({"payment_id": str(payment_id)})
    assert PaymentCreated(payment_id=payment_id).payment_id == payment_id


@pytest.mark.usefixtures("database_url")
async def test_start_binds_retry_and_dlq_queues_to_exchange(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    from payment_processing.consumer import app as consumer_app

    exchange_obj = MagicMock()
    declared: dict[str, MagicMock] = {}

    async def declare_queue(queue: RabbitQueue) -> MagicMock:
        declared[queue.name] = MagicMock(bind=AsyncMock())
        return declared[queue.name]

    monkeypatch.setattr(
        consumer_app.broker, "declare_exchange", AsyncMock(return_value=exchange_obj)
    )
    monkeypatch.setattr(consumer_app.broker, "declare_queue", declare_queue)
    monkeypatch.setattr(consumer_app, "run_relay", MagicMock(return_value=asyncio.sleep(0)))

    await consumer_app.start()
    await consumer_app.stop()

    declared["payments.retry"].bind.assert_awaited_once_with(
        exchange_obj, routing_key="payments.retry"
    )
    declared["payments.dlq"].bind.assert_awaited_once_with(exchange_obj, routing_key="payments.dlq")
    declared["payments.new"].bind.assert_not_called()


async def test_publisher_sends_persistent_messages(monkeypatch: pytest.MonkeyPatch) -> None:
    from payment_processing.consumer import app as consumer_app

    publish = AsyncMock()
    monkeypatch.setattr(consumer_app.broker, "publish", publish)

    await consumer_app.BrokerPublisher().publish(b"{}", "payment.created", message_id="m1")

    assert publish.await_args is not None
    assert publish.await_args.kwargs["persist"] is True
