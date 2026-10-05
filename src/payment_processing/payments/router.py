"""HTTP-ручки платежей."""

import uuid
from typing import Annotated

from fastapi import APIRouter, Depends, Header, HTTPException, status
from sqlalchemy.ext.asyncio import AsyncSession

from payment_processing.db import get_session
from payment_processing.payments import service
from payment_processing.payments.schemas import PaymentAccepted, PaymentCreate, PaymentDetails

router = APIRouter(prefix="/payments", tags=["payments"])

SessionDep = Annotated[AsyncSession, Depends(get_session)]


@router.post("", status_code=status.HTTP_202_ACCEPTED)
async def create_payment(
    data: PaymentCreate,
    session: SessionDep,
    idempotency_key: Annotated[str, Header(min_length=1, max_length=255)],
) -> PaymentAccepted:
    try:
        payment, _ = await service.create_payment(
            session, idempotency_key=idempotency_key, data=data
        )
    except service.IdempotencyConflict as exc:
        raise HTTPException(status.HTTP_409_CONFLICT, detail=str(exc)) from exc
    return PaymentAccepted(
        payment_id=payment.id, status=payment.status, created_at=payment.created_at
    )


@router.get("/{payment_id}")
async def get_payment(payment_id: uuid.UUID, session: SessionDep) -> PaymentDetails:
    payment = await service.get_payment(session, payment_id)
    if payment is None:
        raise HTTPException(status.HTTP_404_NOT_FOUND, detail="payment not found")
    return PaymentDetails.model_validate(payment)
