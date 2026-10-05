"""Процесс consumer: подписчики RabbitMQ и relay outbox (ADR 0001, 0004)."""

import asyncio
import logging
from dataclasses import dataclass

import httpx
from faststream import FastStream
from faststream.middlewares import AckPolicy
from faststream.rabbit import Channel, RabbitBroker, RabbitMessage
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker

from payment_processing.config import get_settings
from payment_processing.consumer.handler import process_payment
from payment_processing.db import get_sessionmaker
from payment_processing.gateway import EmulatedGateway, PaymentGateway
from payment_processing.messaging.schemas import PaymentCreated
from payment_processing.messaging.topology import (
    ATTEMPT_HEADER,
    DLQ_QUEUE,
    EXCHANGE,
    MAX_ATTEMPTS,
    NEW_QUEUE,
    RETRY_QUEUE,
)
from payment_processing.outbox.relay import run_relay

logger = logging.getLogger(__name__)

settings = get_settings()
# Без этого INFO-записи приложения (попытки, retry, DLQ) не попадают в `docker compose logs`.
logging.basicConfig(
    level=settings.log_level, format="%(asctime)s %(levelname)s %(name)s - %(message)s"
)
# Подтверждения публикации включены явно: relay ставит published_at только после ack брокера.
# on_return_raises: сообщение без маршрута (mandatory) не должно считаться отправленным.
broker = RabbitBroker(
    settings.rabbitmq_url,
    # prefetch_count ограничивает число сообщений в работе: каждое держит соединение с БД (ADR 0003).
    default_channel=Channel(
        prefetch_count=settings.consumer_prefetch,
        publisher_confirms=True,
        on_return_raises=True,
    ),
)
app = FastStream(broker)

_relay_task: asyncio.Task[None] | None = None


@dataclass
class Dependencies:
    sessionmaker: async_sessionmaker[AsyncSession]
    gateway: PaymentGateway
    http: httpx.AsyncClient


_deps: Dependencies | None = None


class BrokerPublisher:
    """Публикует события outbox в обменник `payments` и ждёт подтверждения брокера."""

    async def publish(self, body: bytes, routing_key: str, *, message_id: str) -> None:
        await broker.publish(
            body,
            routing_key=routing_key,
            exchange=EXCHANGE,
            message_id=message_id,
            content_type="application/json",
            # Подтверждение брокера считается доставкой только для сохранённого на диск сообщения.
            persist=True,
        )


@broker.subscriber(NEW_QUEUE, EXCHANGE, ack_policy=AckPolicy.MANUAL)
async def handle_payment_created(message: PaymentCreated, msg: RabbitMessage) -> None:
    """Обрабатывает платёж; при ошибке повторяет через `payments.retry` или отправляет в DLQ."""
    assert _deps is not None, "зависимости создаёт after_startup"
    attempt = int(msg.headers.get(ATTEMPT_HEADER, 0))
    payment_id = message.payment_id
    try:
        await process_payment(
            payment_id,
            sessionmaker=_deps.sessionmaker,
            gateway=_deps.gateway,
            http=_deps.http,
        )
    except Exception:
        logger.exception("payment %s attempt %d failed", payment_id, attempt)
        if attempt + 1 < MAX_ATTEMPTS:
            delay = settings.retry_base_delay * 2**attempt
            # Копия уходит в retry раньше ack оригинала: сбой между ними даст дубль, не потерю.
            await broker.publish(
                msg.body,
                queue=RETRY_QUEUE,
                exchange=EXCHANGE,
                headers={ATTEMPT_HEADER: attempt + 1},
                expiration=delay,  # секунды; aio-pika переводит их в TTL в мс
                persist=True,
                content_type="application/json",
            )
            await msg.ack()
            logger.info("payment %s attempt %d: retry in %.1fs", payment_id, attempt, delay)
        else:
            await msg.reject(requeue=False)
            logger.error("payment %s attempt %d: sent to dlq", payment_id, attempt)
        return
    await msg.ack()
    logger.info("payment %s attempt %d: acked", payment_id, attempt)


@app.after_startup
async def start() -> None:
    global _relay_task, _deps
    _deps = Dependencies(
        sessionmaker=get_sessionmaker(),
        gateway=EmulatedGateway.from_settings(),
        http=httpx.AsyncClient(),
    )
    exchange = await broker.declare_exchange(EXCHANGE)
    await broker.declare_queue(NEW_QUEUE)
    # Привязку payments.new делает подписчик; у retry и dlq подписчиков нет, привязываем здесь,
    # иначе dead-letter и повторная публикация в payments уйдут в никуда (ADR 0004).
    for queue in (RETRY_QUEUE, DLQ_QUEUE):
        declared = await broker.declare_queue(queue)
        await declared.bind(exchange, routing_key=queue.routing())
    _relay_task = asyncio.create_task(
        run_relay(get_sessionmaker(), BrokerPublisher(), settings.outbox_poll_interval)
    )


@app.on_shutdown
async def stop() -> None:
    if _relay_task is not None:
        _relay_task.cancel()
        await asyncio.gather(_relay_task, return_exceptions=True)
    if _deps is not None:
        await _deps.http.aclose()
