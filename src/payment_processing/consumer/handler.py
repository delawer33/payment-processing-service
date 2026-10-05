"""Обработка одного платежа: шлюз, терминальный статус, webhook (ADR 0003).

Идемпотентна по `payment_id`: строка платежа блокируется `FOR UPDATE`, статус фиксируется
отдельной транзакцией до webhook, а терминальный платёж шлюз больше не видит.
"""

import logging
import uuid
from datetime import UTC, datetime

import httpx
from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker

from payment_processing.gateway import PaymentGateway
from payment_processing.payments.models import Payment, PaymentStatus
from payment_processing.webhooks import deliver

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
    async with sessionmaker() as session, session.begin():
        payment = await _lock(session, payment_id)
        if payment is None:
            logger.warning("payment %s not found, message dropped", payment_id)
            return
        if payment.status is PaymentStatus.PENDING:
            result = await gateway.charge(payment.id, payment.amount, payment.currency)
            payment.status = PaymentStatus.SUCCEEDED if result.approved else PaymentStatus.FAILED
            payment.gateway_reason = None if result.approved else result.reason
            payment.processed_at = datetime.now(UTC)
            logger.info("payment %s finalised as %s", payment_id, payment.status.value)

    # Второй шаг под той же блокировкой: параллельный дубль не пошлёт webhook второй раз.
    async with sessionmaker() as session, session.begin():
        payment = await _lock(session, payment_id)
        if payment is None or payment.webhook_delivered_at is not None:
            return
        await deliver(http, payment)
        payment.webhook_delivered_at = datetime.now(UTC)
        logger.info("payment %s webhook delivered", payment_id)
