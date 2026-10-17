"""Rate limit по клиенту: фиксированное окно в Redis, 429 с Retry-After, fail open (ADR 0006)."""

import asyncio
import uuid

import pytest
from fastapi import FastAPI
from httpx import AsyncClient
from redis.asyncio import Redis

from payment_processing.config import get_settings
from payment_processing.ratelimit import get_redis, hit

BODY = {
    "amount": "10.00",
    "currency": "RUB",
    "webhook_url": "https://client.example.com/hooks/payments",
}


async def test_allows_up_to_limit_then_denies(redis: Redis) -> None:
    decisions = [await hit(redis, "client-a", limit=3, window=60, now=120.0) for _ in range(4)]

    assert [d.allowed for d in decisions] == [True, True, True, False]
    assert [d.remaining for d in decisions] == [2, 1, 0, 0]
    assert decisions[-1].retry_after == 60


async def test_retry_after_counts_to_window_end(redis: Redis) -> None:
    await hit(redis, "client-a", limit=1, window=60, now=100.0)

    denied = await hit(redis, "client-a", limit=1, window=60, now=100.0)

    # Окно [60, 120): до его конца 20 секунд.
    assert denied.retry_after == 20


async def test_new_window_resets_counter(redis: Redis) -> None:
    await hit(redis, "client-a", limit=1, window=60, now=100.0)

    decision = await hit(redis, "client-a", limit=1, window=60, now=120.0)

    assert decision.allowed


async def test_clients_are_limited_independently(redis: Redis) -> None:
    await hit(redis, "client-a", limit=1, window=60, now=100.0)

    decision = await hit(redis, "client-b", limit=1, window=60, now=100.0)

    assert decision.allowed


async def test_concurrent_requests_wait_for_a_connection_instead_of_failing(redis: Redis) -> None:
    # Пул ограничен; сверх него запрос ждёт соединение, а не падает и не проходит без лимита.
    decisions = await asyncio.gather(
        *(hit(redis, "client-a", limit=1000, window=60, now=100.0) for _ in range(300))
    )

    assert sorted(d.remaining for d in decisions) == list(range(700, 1000))


async def test_api_answers_429_with_retry_after(
    client: AsyncClient, monkeypatch: pytest.MonkeyPatch
) -> None:
    monkeypatch.setattr(get_settings(), "rate_limit_requests", 2)

    statuses = []
    for _ in range(3):
        response = await client.post(
            "/api/v1/payments", json=BODY, headers={"Idempotency-Key": str(uuid.uuid4())}
        )
        statuses.append(response.status_code)

    assert statuses == [202, 202, 429]
    assert int(response.headers["Retry-After"]) >= 1
    assert response.json() == {"detail": "rate limit of 2 requests per 1s exceeded"}


async def test_api_reports_remaining_quota(
    client: AsyncClient, monkeypatch: pytest.MonkeyPatch
) -> None:
    monkeypatch.setattr(get_settings(), "rate_limit_requests", 5)

    response = await client.post(
        "/api/v1/payments", json=BODY, headers={"Idempotency-Key": str(uuid.uuid4())}
    )

    assert response.status_code == 202
    assert response.headers["X-RateLimit-Limit"] == "5"
    assert response.headers["X-RateLimit-Remaining"] == "4"


async def test_unreachable_redis_lets_requests_through(app: FastAPI, client: AsyncClient) -> None:
    # Порт 1 никто не слушает: соединение с Redis не установится.
    unreachable = Redis.from_url("redis://127.0.0.1:1/0", socket_connect_timeout=0.2)
    app.dependency_overrides[get_redis] = lambda: unreachable

    response = await client.post(
        "/api/v1/payments", json=BODY, headers={"Idempotency-Key": str(uuid.uuid4())}
    )

    await unreachable.aclose()
    assert response.status_code == 202
    assert "X-RateLimit-Remaining" not in response.headers
