"""Топология RabbitMQ (ADR 0004): аргументы очередей и доставка подписчику."""

import uuid

import pytest
from faststream.rabbit import TestRabbitBroker

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
async def test_topology_declares_three_queues_with_dlx_arguments() -> None:
    from payment_processing.consumer.app import broker, handle_payment_created

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
