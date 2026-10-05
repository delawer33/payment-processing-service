"""Доставка webhook: подписанный POST с итоговым статусом платежа."""

import asyncio
import hashlib
import hmac
import json

import httpx

from payment_processing.config import get_settings
from payment_processing.payments.models import Payment


class WebhookDeliveryError(Exception):
    """Webhook не доставлен: сетевая ошибка, таймаут или не-2xx ответ."""


def sign(secret: str, body: bytes) -> str:
    return "sha256=" + hmac.new(secret.encode(), body, hashlib.sha256).hexdigest()


def build_body(payment: Payment) -> bytes:
    assert payment.processed_at is not None, "webhook отправляется только после фиксации статуса"
    payload = {
        "payment_id": str(payment.id),
        "status": payment.status.value,
        "amount": str(payment.amount),
        "currency": payment.currency,
        "processed_at": payment.processed_at.isoformat(),
        "metadata": payment.metadata_,
    }
    return json.dumps(payload, separators=(",", ":"), ensure_ascii=False).encode()


async def deliver(client: httpx.AsyncClient, payment: Payment) -> None:
    settings = get_settings()
    body = build_body(payment)
    headers = {
        "Content-Type": "application/json",
        "X-Signature": sign(settings.webhook_secret, body),
    }
    try:
        # Таймаут httpx ограничивает каждую фазу (connect, read...) отдельно; медленно капающий
        # ответ уложится в каждую и растянет доставку. Общий дедлайн держит prefetch-слот в рамках.
        async with asyncio.timeout(settings.webhook_timeout):
            response = await client.post(
                payment.webhook_url,
                content=body,
                headers=headers,
                timeout=settings.webhook_timeout,
            )
    except httpx.HTTPError as exc:
        raise WebhookDeliveryError(f"webhook {payment.webhook_url} unreachable: {exc!r}") from exc
    except TimeoutError as exc:
        raise WebhookDeliveryError(
            f"webhook {payment.webhook_url} timed out after {settings.webhook_timeout}s"
        ) from exc
    if not response.is_success:
        raise WebhookDeliveryError(f"webhook {payment.webhook_url} answered {response.status_code}")
