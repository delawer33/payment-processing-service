"""Процесс consumer: подписчики RabbitMQ и relay outbox (ADR 0001, 0004)."""

import asyncio
import logging

from faststream import FastStream
from faststream.rabbit import Channel, RabbitBroker

from payment_processing.config import get_settings
from payment_processing.db import get_sessionmaker
from payment_processing.messaging.schemas import PaymentCreated
from payment_processing.messaging.topology import (
    DLQ_QUEUE,
    EXCHANGE,
    NEW_QUEUE,
    RETRY_QUEUE,
)
from payment_processing.outbox.relay import run_relay

logger = logging.getLogger(__name__)

settings = get_settings()
# Подтверждения публикации включены явно: relay ставит published_at только после ack брокера.
# on_return_raises: сообщение без маршрута (mandatory) не должно считаться отправленным.
broker = RabbitBroker(
    settings.rabbitmq_url,
    default_channel=Channel(publisher_confirms=True, on_return_raises=True),
)
app = FastStream(broker)

_relay_task: asyncio.Task[None] | None = None


class BrokerPublisher:
    """Публикует события outbox в обменник `payments` и ждёт подтверждения брокера."""

    async def publish(self, body: bytes, routing_key: str, *, message_id: str) -> None:
        await broker.publish(
            body,
            routing_key=routing_key,
            exchange=EXCHANGE,
            message_id=message_id,
            content_type="application/json",
        )


@broker.subscriber(NEW_QUEUE, EXCHANGE)
async def handle_payment_created(message: PaymentCreated) -> None:
    logger.info("payment created event received: %s", message.payment_id)


@app.after_startup
async def start() -> None:
    global _relay_task
    await broker.declare_exchange(EXCHANGE)
    for queue in (NEW_QUEUE, RETRY_QUEUE, DLQ_QUEUE):
        await broker.declare_queue(queue)
    _relay_task = asyncio.create_task(
        run_relay(get_sessionmaker(), BrokerPublisher(), settings.outbox_poll_interval)
    )


@app.on_shutdown
async def stop() -> None:
    if _relay_task is not None:
        _relay_task.cancel()
        await asyncio.gather(_relay_task, return_exceptions=True)
