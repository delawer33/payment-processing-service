"""Платёжный шлюз: интерфейс и эмуляция с задержкой и долей отказов (ADR 0003)."""

import asyncio
import random
from dataclasses import dataclass
from decimal import Decimal
from typing import Protocol
from uuid import UUID

from payment_processing.config import get_settings


@dataclass(frozen=True, slots=True)
class GatewayResult:
    """Бизнес-исход списания. Отказ — не ошибка: исключение означает сбой транспорта."""

    approved: bool
    reason: str | None = None


class PaymentGateway(Protocol):
    async def charge(self, payment_id: UUID, amount: Decimal, currency: str) -> GatewayResult: ...


class EmulatedGateway:
    """Ждёт случайное время из диапазона и одобряет платёж с вероятностью `success_rate`."""

    def __init__(
        self,
        delay_min: float,
        delay_max: float,
        success_rate: float,
        rng: random.Random | None = None,
    ) -> None:
        self._delay_min = delay_min
        self._delay_max = delay_max
        self._success_rate = success_rate
        self._rng = rng or random.Random()

    @classmethod
    def from_settings(cls) -> "EmulatedGateway":
        settings = get_settings()
        return cls(
            settings.gateway_delay_min,
            settings.gateway_delay_max,
            settings.gateway_success_rate,
        )

    async def charge(
        self,
        payment_id: UUID,  # noqa: ARG002 — эмуляция не смотрит на платёж, интерфейс общий
        amount: Decimal,  # noqa: ARG002
        currency: str,  # noqa: ARG002
    ) -> GatewayResult:
        await asyncio.sleep(self._rng.uniform(self._delay_min, self._delay_max))
        if self._rng.random() < self._success_rate:
            return GatewayResult(approved=True)
        return GatewayResult(approved=False, reason="declined by issuer")
