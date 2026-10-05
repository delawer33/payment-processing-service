"""Создание и чтение платежей. Транзакцией владеет `get_session`, сервис не коммитит."""

import hashlib
import json
import uuid

from sqlalchemy import select
from sqlalchemy.dialects.postgresql import insert
from sqlalchemy.ext.asyncio import AsyncSession

from payment_processing.outbox.models import OutboxEvent
from payment_processing.payments.models import Payment
from payment_processing.payments.schemas import PaymentCreate


class IdempotencyConflict(Exception):
    """Ключ идемпотентности уже использован для платежа с другим телом запроса."""

    def __init__(self, idempotency_key: str) -> None:
        super().__init__(
            f"idempotency key {idempotency_key!r} was already used with a different request body"
        )
        self.idempotency_key = idempotency_key


def request_hash(data: PaymentCreate) -> str:
    canonical = json.dumps(data.model_dump(mode="json"), sort_keys=True, separators=(",", ":"))
    return hashlib.sha256(canonical.encode()).hexdigest()


async def create_payment(
    session: AsyncSession, *, idempotency_key: str, data: PaymentCreate
) -> tuple[Payment, bool]:
    """Создаёт платёж или возвращает существующий по ключу (ADR 0002).

    Второй элемент результата — создан ли платёж этим вызовом.
    """
    digest = request_hash(data)
    stmt = (
        insert(Payment)
        .values(
            id=uuid.uuid4(),
            amount=data.amount,
            currency=data.currency,
            description=data.description,
            metadata_=data.metadata,
            idempotency_key=idempotency_key,
            request_hash=digest,
            webhook_url=str(data.webhook_url),
        )
        .on_conflict_do_nothing(index_elements=["idempotency_key"])
        .returning(Payment)
    )
    payment = (await session.execute(stmt)).scalar_one_or_none()
    if payment is not None:
        session.add(
            OutboxEvent(
                aggregate_id=payment.id,
                event_type="payment.created",
                payload={"payment_id": str(payment.id)},
            )
        )
        return payment, True

    existing = (
        await session.execute(select(Payment).where(Payment.idempotency_key == idempotency_key))
    ).scalar_one()
    if existing.request_hash != digest:
        raise IdempotencyConflict(idempotency_key)
    return existing, False


async def get_payment(session: AsyncSession, payment_id: uuid.UUID) -> Payment | None:
    return await session.get(Payment, payment_id)
