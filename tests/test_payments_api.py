import asyncio
import uuid
from typing import Any

import pytest
from httpx import AsyncClient
from sqlalchemy import text
from sqlalchemy.exc import DBAPIError
from sqlalchemy.ext.asyncio import AsyncEngine

BODY: dict[str, Any] = {
    "amount": "100.50",
    "currency": "RUB",
    "description": "Заказ 42",
    "metadata": {"order_id": 42},
    "webhook_url": "https://client.example.com/hooks/payments",
}


def headers(key: str | None = None) -> dict[str, str]:
    return {"Idempotency-Key": key or str(uuid.uuid4())}


async def count(engine: AsyncEngine, table: str, where: str = "true") -> int:
    async with engine.connect() as conn:
        result = await conn.execute(text(f"SELECT count(*) FROM {table} WHERE {where}"))
        return int(result.scalar_one())


async def test_create_payment_returns_202_and_persists(client: AsyncClient) -> None:
    response = await client.post("/api/v1/payments", json=BODY, headers=headers())

    assert response.status_code == 202
    accepted = response.json()
    assert accepted["status"] == "pending"
    assert uuid.UUID(accepted["payment_id"])
    assert accepted["created_at"]


async def test_create_writes_payment_and_outbox_in_one_transaction(
    client: AsyncClient, engine: AsyncEngine
) -> None:
    response = await client.post("/api/v1/payments", json=BODY, headers=headers())

    payment_id = response.json()["payment_id"]
    assert await count(engine, "payments") == 1
    assert await count(engine, "outbox", "published_at IS NULL") == 1
    async with engine.connect() as conn:
        row = (
            await conn.execute(text("SELECT aggregate_id, event_type, payload FROM outbox"))
        ).one()
    assert str(row.aggregate_id) == payment_id
    assert row.event_type == "payment.created"
    assert row.payload == {"payment_id": payment_id}


async def test_same_idempotency_key_returns_same_payment(
    client: AsyncClient, engine: AsyncEngine
) -> None:
    h = headers("key-1")

    first = await client.post("/api/v1/payments", json=BODY, headers=h)
    second = await client.post("/api/v1/payments", json=BODY, headers=h)

    assert first.status_code == second.status_code == 202
    assert first.json()["payment_id"] == second.json()["payment_id"]
    assert await count(engine, "payments") == 1
    assert await count(engine, "outbox") == 1


async def test_same_idempotency_key_with_equivalent_amount_returns_same_payment(
    client: AsyncClient, engine: AsyncEngine
) -> None:
    h = headers("key-equivalent-amount")

    first = await client.post("/api/v1/payments", json=BODY, headers=h)
    second = await client.post("/api/v1/payments", json={**BODY, "amount": "100.5"}, headers=h)

    assert first.status_code == second.status_code == 202
    assert first.json()["payment_id"] == second.json()["payment_id"]
    assert await count(engine, "payments") == 1


async def test_same_idempotency_key_concurrent_requests_create_one_payment(
    client: AsyncClient, engine: AsyncEngine
) -> None:
    h = headers("key-concurrent")

    responses = await asyncio.gather(
        *(client.post("/api/v1/payments", json=BODY, headers=h) for _ in range(10))
    )

    assert {r.status_code for r in responses} == {202}
    assert len({r.json()["payment_id"] for r in responses}) == 1
    assert await count(engine, "payments") == 1
    assert await count(engine, "outbox") == 1


async def test_same_idempotency_key_different_body_is_409(client: AsyncClient) -> None:
    h = headers("key-2")
    await client.post("/api/v1/payments", json=BODY, headers=h)

    response = await client.post("/api/v1/payments", json={**BODY, "amount": "1.00"}, headers=h)

    assert response.status_code == 409


async def test_missing_idempotency_key_is_422(client: AsyncClient) -> None:
    response = await client.post("/api/v1/payments", json=BODY)

    assert response.status_code == 422


async def test_missing_api_key_is_401(client: AsyncClient) -> None:
    del client.headers["X-API-Key"]

    response = await client.post("/api/v1/payments", json=BODY, headers=headers())

    assert response.status_code == 401
    assert response.json() == {"detail": "invalid API key"}


async def test_wrong_api_key_is_401(client: AsyncClient) -> None:
    client.headers["X-API-Key"] = "wrong"

    response = await client.get("/api/v1/health")

    assert response.status_code == 401
    assert response.json() == {"detail": "invalid API key"}


async def test_health_with_api_key(client: AsyncClient) -> None:
    response = await client.get("/api/v1/health")

    assert response.json() == {"status": "ok"}


async def test_get_unknown_payment_is_404(client: AsyncClient) -> None:
    response = await client.get(f"/api/v1/payments/{uuid.uuid4()}")

    assert response.status_code == 404


async def test_get_payment_returns_details(client: AsyncClient) -> None:
    created = await client.post("/api/v1/payments", json=BODY, headers=headers("key-3"))

    response = await client.get(f"/api/v1/payments/{created.json()['payment_id']}")

    assert response.status_code == 200
    details = response.json()
    assert details["id"] == created.json()["payment_id"]
    assert details["amount"] == "100.50"
    assert details["currency"] == "RUB"
    assert details["metadata"] == {"order_id": 42}
    assert details["status"] == "pending"
    assert details["idempotency_key"] == "key-3"
    assert details["processed_at"] is None
    assert details["webhook_delivered_at"] is None
    assert details["gateway_reason"] is None
    assert "request_hash" not in details


@pytest.mark.parametrize("amount", ["0", "-1", "1.234"])
async def test_amount_must_be_positive_with_two_decimals(client: AsyncClient, amount: str) -> None:
    response = await client.post(
        "/api/v1/payments", json={**BODY, "amount": amount}, headers=headers()
    )

    assert response.status_code == 422


async def test_amount_too_large_is_422(client: AsyncClient) -> None:
    """NUMERIC(18,2) держит 16 цифр до запятой; больше — ошибка клиента, а не 500 из БД."""
    response = await client.post(
        "/api/v1/payments", json={**BODY, "amount": "1e16"}, headers=headers()
    )

    assert response.status_code == 422


async def test_terminal_status_cannot_be_changed_in_db(
    client: AsyncClient, engine: AsyncEngine
) -> None:
    created = await client.post("/api/v1/payments", json=BODY, headers=headers())
    payment_id = created.json()["payment_id"]
    async with engine.begin() as conn:
        await conn.execute(
            text("UPDATE payments SET status = 'succeeded' WHERE id = :id"), {"id": payment_id}
        )

    with pytest.raises(DBAPIError, match="already succeeded"):
        async with engine.begin() as conn:
            await conn.execute(
                text("UPDATE payments SET status = 'failed' WHERE id = :id"), {"id": payment_id}
            )
