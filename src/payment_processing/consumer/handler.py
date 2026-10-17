"""Обработка одного платежа: шлюз, терминальный статус, webhook (ADR 0003).

Идемпотентна по `payment_id`: строка платежа блокируется `FOR UPDATE`, статус фиксируется
отдельной транзакцией до webhook, а терминальный платёж шлюз больше не видит.
"""

import logging
import time
import uuid
from datetime import UTC, datetime

import httpx
from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker

from payment_processing.consumer import metrics
from payment_processing.gateway import PaymentGateway
from payment_processing.payments.models import Payment, PaymentStatus
from payment_processing.webhooks import WebhookDeliveryError, deliver

logger = logging.getLogger(__name__)


async def _lock(session: AsyncSession, payment_id: uuid.UUID) -> Payment | None:
    stmt = select(Payment).where(Payment.id == payment_id).with_for_update()
    return (await session.execute(stmt)).scalar_one_or_none()


async def process_payment(
    payment_id: uuid.UUID,
    *,
    sessionmaker: async_sessionmaker[AsyncSession],
    gateway: PaymentGateway,
    http: httpx.AsyncClient,
) -> None:
    """Доводит платёж до терминального статуса и доставляет webhook.

    Исключение шлюза (сбой транспорта) откатывает транзакцию: платёж остаётся `pending` и
    уходит в retry. `WebhookDeliveryError` приходит уже после коммита статуса: повтор
    пропустит шлюз и отправит только webhook.
    """
    finalised: PaymentStatus | None = None
    async with sessionmaker() as session, session.begin():
        payment = await _lock(session, payment_id)
        if payment is None:
            logger.warning("payment %s not found, message dropped", payment_id)
            return
        if payment.status is PaymentStatus.PENDING:
            started = time.perf_counter()
            try:
                result = await gateway.charge(payment.id, payment.amount, payment.currency)
            finally:
                metrics.GATEWAY_DURATION.observe(time.perf_counter() - started)
            payment.status = PaymentStatus.SUCCEEDED if result.approved else PaymentStatus.FAILED
            payment.gateway_reason = None if result.approved else result.reason
            payment.processed_at = datetime.now(UTC)
            finalised = payment.status
            logger.info("payment %s finalised as %s", payment_id, payment.status.value)
    # Счётчик после коммита: откатившаяся транзакция не попадает в метрики.
    if finalised is not None:
        metrics.PAYMENTS_FINALISED.labels(finalised.value).inc()

    # Второй шаг под той же блокировкой: параллельный дубль не пошлёт webhook второй раз.
    async with sessionmaker() as session, session.begin():
        payment = await _lock(session, payment_id)
        if payment is None or payment.webhook_delivered_at is not None:
            return
        try:
            await deliver(http, payment)
        except WebhookDeliveryError:
            metrics.WEBHOOKS.labels("failed").inc()
            raise
        payment.webhook_delivered_at = datetime.now(UTC)
        logger.info("payment %s webhook delivered", payment_id)
    metrics.WEBHOOKS.labels("delivered").inc()
