"""Обработчик платежа: шлюз, терминальный статус, webhook (ADR 0003)."""

import asyncio
import hashlib
import hmac
import json
import uuid
from decimal import Decimal

import httpx
import pytest
from sqlalchemy.ext.asyncio import AsyncEngine, AsyncSession, async_sessionmaker

from payment_processing.consumer.handler import process_payment
from payment_processing.gateway import GatewayResult
from payment_processing.payments.models import Payment, PaymentStatus
from payment_processing.webhooks import WebhookDeliveryError

WEBHOOK_URL = "https://merchant.example/hooks/payments"


class FakeGateway:
    def __init__(
        self,
        result: GatewayResult | None = None,
        error: Exception | None = None,
        delay: float = 0.0,
    ) -> None:
        self.result = result or GatewayResult(approved=True)
        self.error = error
        self.delay = delay
        self.calls = 0

    async def charge(self, payment_id: uuid.UUID, amount: Decimal, currency: str) -> GatewayResult:
        self.calls += 1
        await asyncio.sleep(self.delay)
        if self.error is not None:
            raise self.error
        return self.result


class WebhookReceiver:
    def __init__(self, status_code: int = 200) -> None:
        self.status_code = status_code
        self.requests: list[httpx.Request] = []
        self.client = httpx.AsyncClient(transport=httpx.MockTransport(self._handle))

    def _handle(self, request: httpx.Request) -> httpx.Response:
        self.requests.append(request)
        return httpx.Response(self.status_code)


@pytest.fixture
def sessionmaker(engine: AsyncEngine) -> async_sessionmaker[AsyncSession]:
    return async_sessionmaker(engine, expire_on_commit=False)


@pytest.fixture
async def receiver() -> WebhookReceiver:
    return WebhookReceiver()


async def _add_payment(
    sessionmaker: async_sessionmaker[AsyncSession],
    status: PaymentStatus = PaymentStatus.PENDING,
) -> uuid.UUID:
    payment = Payment(
        amount=Decimal("100.50"),
        currency="RUB",
        metadata_={"order": "42"},
        idempotency_key=str(uuid.uuid4()),
        request_hash="0" * 64,
        webhook_url=WEBHOOK_URL,
    )
    async with sessionmaker() as session:
        session.add(payment)
        await session.commit()
        payment_id = payment.id
    if status is not PaymentStatus.PENDING:
        async with sessionmaker() as session:
            stored = await session.get_one(Payment, payment_id)
            stored.status = status
            stored.processed_at = stored.created_at
            await session.commit()
    return payment_id


async def _load(sessionmaker: async_sessionmaker[AsyncSession], payment_id: uuid.UUID) -> Payment:
    async with sessionmaker() as session:
        return await session.get_one(Payment, payment_id)


async def test_success_sets_succeeded_processed_at_and_delivers_webhook(
    sessionmaker: async_sessionmaker[AsyncSession], receiver: WebhookReceiver
) -> None:
    payment_id = await _add_payment(sessionmaker)
    gateway = FakeGateway()

    await process_payment(
        payment_id, sessionmaker=sessionmaker, gateway=gateway, http=receiver.client
    )

    payment = await _load(sessionmaker, payment_id)
    assert payment.status is PaymentStatus.SUCCEEDED
    assert payment.processed_at is not None
    assert payment.webhook_delivered_at is not None
    assert gateway.calls == 1

    (request,) = receiver.requests
    assert str(request.url) == WEBHOOK_URL
    assert json.loads(request.content) == {
        "payment_id": str(payment_id),
        "status": "succeeded",
        "amount": "100.50",
        "currency": "RUB",
        "processed_at": payment.processed_at.isoformat(),
        "metadata": {"order": "42"},
    }
    expected = hmac.new(b"change-me", request.content, hashlib.sha256).hexdigest()
    assert request.headers["X-Signature"] == f"sha256={expected}"
    assert request.headers["Content-Type"] == "application/json"


async def test_gateway_decline_sets_failed_and_delivers_webhook(
    sessionmaker: async_sessionmaker[AsyncSession], receiver: WebhookReceiver
) -> None:
    payment_id = await _add_payment(sessionmaker)
    gateway = FakeGateway(GatewayResult(approved=False, reason="insufficient funds"))

    await process_payment(
        payment_id, sessionmaker=sessionmaker, gateway=gateway, http=receiver.client
    )

    payment = await _load(sessionmaker, payment_id)
    assert payment.status is PaymentStatus.FAILED
    assert payment.gateway_reason == "insufficient funds"
    assert payment.webhook_delivered_at is not None
    assert gateway.calls == 1
    assert json.loads(receiver.requests[0].content)["status"] == "failed"


async def test_redelivery_of_finalised_payment_does_not_call_gateway(
    sessionmaker: async_sessionmaker[AsyncSession], receiver: WebhookReceiver
) -> None:
    """Главный инвариант: платёж уже succeeded, webhook не ушёл — шлюз не трогаем."""
    payment_id = await _add_payment(sessionmaker, PaymentStatus.SUCCEEDED)
    gateway = FakeGateway(GatewayResult(approved=False, reason="would flip status"))

    await process_payment(
        payment_id, sessionmaker=sessionmaker, gateway=gateway, http=receiver.client
    )

    payment = await _load(sessionmaker, payment_id)
    assert gateway.calls == 0
    assert payment.status is PaymentStatus.SUCCEEDED
    assert payment.webhook_delivered_at is not None
    assert len(receiver.requests) == 1


async def test_redelivery_after_webhook_delivered_does_nothing(
    sessionmaker: async_sessionmaker[AsyncSession], receiver: WebhookReceiver
) -> None:
    payment_id = await _add_payment(sessionmaker)
    gateway = FakeGateway()
    for _ in range(2):
        await process_payment(
            payment_id, sessionmaker=sessionmaker, gateway=gateway, http=receiver.client
        )

    assert gateway.calls == 1
    assert len(receiver.requests) == 1


async def test_concurrent_duplicate_messages_call_gateway_once(
    sessionmaker: async_sessionmaker[AsyncSession], receiver: WebhookReceiver
) -> None:
    payment_id = await _add_payment(sessionmaker)
    gateway = FakeGateway(delay=0.2)

    await asyncio.gather(
        *(
            process_payment(
                payment_id, sessionmaker=sessionmaker, gateway=gateway, http=receiver.client
            )
            for _ in range(2)
        )
    )

    assert gateway.calls == 1
    assert len(receiver.requests) == 1
    assert (await _load(sessionmaker, payment_id)).status is PaymentStatus.SUCCEEDED


async def test_webhook_failure_keeps_terminal_status_and_raises(
    sessionmaker: async_sessionmaker[AsyncSession],
) -> None:
    payment_id = await _add_payment(sessionmaker)
    broken = WebhookReceiver(status_code=503)

    with pytest.raises(WebhookDeliveryError):
        await process_payment(
            payment_id, sessionmaker=sessionmaker, gateway=FakeGateway(), http=broken.client
        )

    payment = await _load(sessionmaker, payment_id)
    assert payment.status is PaymentStatus.SUCCEEDED
    assert payment.webhook_delivered_at is None


async def test_webhook_network_error_raises_delivery_error(
    sessionmaker: async_sessionmaker[AsyncSession],
) -> None:
    def refuse(request: httpx.Request) -> httpx.Response:
        raise httpx.ConnectError("connection refused", request=request)

    payment_id = await _add_payment(sessionmaker)
    client = httpx.AsyncClient(transport=httpx.MockTransport(refuse))

    with pytest.raises(WebhookDeliveryError):
        await process_payment(
            payment_id, sessionmaker=sessionmaker, gateway=FakeGateway(), http=client
        )


async def test_gateway_transport_error_keeps_pending(
    sessionmaker: async_sessionmaker[AsyncSession], receiver: WebhookReceiver
) -> None:
    payment_id = await _add_payment(sessionmaker)
    gateway = FakeGateway(error=TimeoutError("gateway timed out"))

    with pytest.raises(TimeoutError):
        await process_payment(
            payment_id, sessionmaker=sessionmaker, gateway=gateway, http=receiver.client
        )

    payment = await _load(sessionmaker, payment_id)
    assert payment.status is PaymentStatus.PENDING
    assert payment.processed_at is None
    assert receiver.requests == []


async def test_unknown_payment_is_acked_without_error(
    sessionmaker: async_sessionmaker[AsyncSession], receiver: WebhookReceiver
) -> None:
    gateway = FakeGateway()

    await process_payment(
        uuid.uuid4(), sessionmaker=sessionmaker, gateway=gateway, http=receiver.client
    )

    assert gateway.calls == 0
    assert receiver.requests == []
