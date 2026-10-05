"""Тела сообщений RabbitMQ."""

import uuid

from pydantic import BaseModel


class PaymentCreated(BaseModel):
    payment_id: uuid.UUID
