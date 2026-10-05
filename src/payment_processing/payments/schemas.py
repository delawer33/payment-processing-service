"""Контракт API платежей."""

import uuid
from datetime import datetime
from decimal import Decimal
from typing import Annotated, Any, Literal

from pydantic import BaseModel, ConfigDict, Field, HttpUrl, PlainSerializer

from payment_processing.payments.models import PaymentStatus

# Сумма уходит клиенту строкой: JSON-число потеряло бы точность на стороне клиента.
MoneyStr = Annotated[Decimal, PlainSerializer(lambda v: str(v), return_type=str, when_used="json")]


class PaymentCreate(BaseModel):
    amount: Annotated[MoneyStr, Field(gt=0, decimal_places=2)]
    currency: Literal["RUB", "USD", "EUR"]
    description: str = Field(default="", max_length=255)
    metadata: dict[str, Any] = Field(default_factory=dict)
    webhook_url: HttpUrl


class PaymentAccepted(BaseModel):
    payment_id: uuid.UUID
    status: PaymentStatus
    created_at: datetime


class PaymentDetails(BaseModel):
    model_config = ConfigDict(from_attributes=True)

    id: uuid.UUID
    amount: MoneyStr
    currency: str
    description: str
    metadata: dict[str, Any] = Field(validation_alias="metadata_")
    status: PaymentStatus
    idempotency_key: str
    webhook_url: str
    created_at: datetime
    processed_at: datetime | None
    webhook_delivered_at: datetime | None
    gateway_reason: str | None
