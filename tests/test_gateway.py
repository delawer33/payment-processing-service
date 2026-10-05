"""Эмулированный шлюз: детерминированный генератор, диапазон задержки, доля отказов."""

import random
import uuid
from decimal import Decimal

import pytest

from payment_processing.gateway import EmulatedGateway


async def _charge(gateway: EmulatedGateway) -> bool:
    return (await gateway.charge(uuid.uuid4(), Decimal("1.00"), "RUB")).approved


async def test_success_rate_one_always_approves_and_zero_always_declines() -> None:
    always = EmulatedGateway(0, 0, 1.0, random.Random(1))
    never = EmulatedGateway(0, 0, 0.0, random.Random(1))

    assert await _charge(always)
    result = await never.charge(uuid.uuid4(), Decimal("1.00"), "RUB")
    assert not result.approved
    assert result.reason


async def test_delay_is_drawn_from_configured_range(monkeypatch: pytest.MonkeyPatch) -> None:
    slept: list[float] = []

    async def fake_sleep(delay: float) -> None:
        slept.append(delay)

    monkeypatch.setattr("payment_processing.gateway.asyncio.sleep", fake_sleep)
    gateway = EmulatedGateway(2.0, 5.0, 1.0, random.Random(7))

    for _ in range(20):
        await _charge(gateway)

    assert all(2.0 <= d <= 5.0 for d in slept)
    assert len(set(slept)) > 1
