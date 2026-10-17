"""Метрики API: `/metrics` без ключа, запросы по шаблону маршрута, исходы создания платежа."""

import uuid

from httpx import AsyncClient
from prometheus_client import REGISTRY

BODY = {
    "amount": "10.00",
    "currency": "RUB",
    "webhook_url": "https://client.example.com/hooks/payments",
}


def metric(name: str, **labels: str) -> float:
    return REGISTRY.get_sample_value(name, labels) or 0.0


async def test_metrics_endpoint_needs_no_api_key(client: AsyncClient) -> None:
    response = await client.get("/metrics", headers={"X-API-Key": ""})

    assert response.status_code == 200
    assert "http_requests_total" in response.text


async def test_requests_are_labelled_by_route_template(client: AsyncClient) -> None:
    labels = {"method": "GET", "route": "/api/v1/payments/{payment_id}", "status": "404"}
    before = metric("http_requests_total", **labels)

    await client.get(f"/api/v1/payments/{uuid.uuid4()}")

    assert metric("http_requests_total", **labels) == before + 1


async def test_unmatched_paths_are_not_counted(client: AsyncClient) -> None:
    before = sum(
        s.value
        for m in REGISTRY.collect()
        if m.name == "http_requests"
        for s in m.samples
        if s.name == "http_requests_total"
    )

    await client.get("/wp-login.php")

    after = sum(
        s.value
        for m in REGISTRY.collect()
        if m.name == "http_requests"
        for s in m.samples
        if s.name == "http_requests_total"
    )
    assert after == before


async def test_payment_creation_outcomes_are_counted(client: AsyncClient) -> None:
    before = {r: metric("payments_accepted_total", result=r) for r in ("created", "replayed")}
    h = {"Idempotency-Key": str(uuid.uuid4())}

    await client.post("/api/v1/payments", json=BODY, headers=h)
    await client.post("/api/v1/payments", json=BODY, headers=h)
    await client.post("/api/v1/payments", json={**BODY, "amount": "11.00"}, headers=h)

    assert metric("payments_accepted_total", result="created") == before["created"] + 1
    assert metric("payments_accepted_total", result="replayed") == before["replayed"] + 1
    assert metric("payments_accepted_total", result="conflict") >= 1
