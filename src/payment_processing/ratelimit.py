"""Rate limit на клиента: фиксированное окно, счётчик в Redis (ADR 0006).

Клиент — это его API-ключ; в Redis лежит только префикс SHA-256 ключа, не сам ключ.
Redis недоступен — запрос пропускается: лимит защищает сервис от перегрузки, а не деньги,
и его отказ не должен останавливать приём платежей.
"""

import hashlib
import logging
import math
import time
from dataclasses import dataclass
from functools import lru_cache
from typing import Annotated

from fastapi import Depends, Header, HTTPException, Response, status
from redis.asyncio import BlockingConnectionPool, Redis
from redis.exceptions import RedisError

from payment_processing import metrics
from payment_processing.config import Settings, get_settings

logger = logging.getLogger(__name__)


@dataclass(frozen=True, slots=True)
class Decision:
    allowed: bool
    remaining: int
    # Секунды до конца текущего окна; клиент получает их в `Retry-After`.
    retry_after: int


@lru_cache
def get_redis() -> Redis:
    settings = get_settings()
    # Обычный пул сверх `max_connections` сразу бросает ошибку, и под нагрузкой лимит молча
    # отключался бы. Блокирующий ждёт свободное соединение, но не дольше `redis_timeout`.
    pool = BlockingConnectionPool.from_url(
        settings.redis_url,
        max_connections=settings.redis_max_connections,
        timeout=settings.redis_timeout,
        socket_timeout=settings.redis_timeout,
        socket_connect_timeout=settings.redis_timeout,
    )
    return Redis(connection_pool=pool)


async def hit(redis: Redis, client: str, *, limit: int, window: int, now: float) -> Decision:
    """Засчитывает запрос клиента в окно, которому принадлежит `now`, и решает, пропустить ли его."""
    window_index = int(now // window)
    key = f"ratelimit:{client}:{window}:{window_index}"
    async with redis.pipeline(transaction=True) as pipe:
        # NX: срок жизни ставит только первый запрос окна, остальные его не продлевают.
        pipe.incr(key)
        pipe.expire(key, window, nx=True)
        count, _ = await pipe.execute()
    retry_after = max(1, math.ceil((window_index + 1) * window - now))
    return Decision(
        allowed=count <= limit, remaining=max(0, limit - count), retry_after=retry_after
    )


def client_id(api_key: str) -> str:
    return hashlib.sha256(api_key.encode()).hexdigest()[:16]


async def enforce_rate_limit(
    response: Response,
    settings: Annotated[Settings, Depends(get_settings)],
    redis: Annotated[Redis, Depends(get_redis)],
    x_api_key: Annotated[str, Header()],
) -> None:
    """Зависимость роутера: 429 с `Retry-After` сверх лимита, иначе заголовки остатка квоты."""
    limit, window = settings.rate_limit_requests, settings.rate_limit_window
    try:
        decision = await hit(
            redis, client_id(x_api_key), limit=limit, window=window, now=time.time()
        )
    except RedisError as exc:
        # Без traceback: под нагрузкой эта строка пишется на каждый запрос.
        logger.warning("rate limit backend unavailable, request let through: %r", exc)
        metrics.RATE_LIMIT_BACKEND_ERRORS.inc()
        return
    if not decision.allowed:
        metrics.RATE_LIMIT_REJECTIONS.inc()
        raise HTTPException(
            status.HTTP_429_TOO_MANY_REQUESTS,
            detail=f"rate limit of {limit} requests per {window}s exceeded",
            headers={"Retry-After": str(decision.retry_after)},
        )
    response.headers["X-RateLimit-Limit"] = str(limit)
    response.headers["X-RateLimit-Remaining"] = str(decision.remaining)
