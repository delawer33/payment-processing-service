"""Relay: переносит записи outbox в брокер at-least-once (ADR 0001)."""

import asyncio
import json
import logging
from datetime import UTC, datetime
from typing import Protocol

from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker

from payment_processing.config import get_settings
from payment_processing.messaging.topology import NEW_ROUTING_KEY
from payment_processing.outbox.models import OutboxEvent

logger = logging.getLogger(__name__)

ROUTING_KEYS = {"payment.created": NEW_ROUTING_KEY}


class Publisher(Protocol):
    async def publish(self, body: bytes, routing_key: str, *, message_id: str) -> None: ...


async def relay_once(session: AsyncSession, publisher: Publisher, *, batch_size: int) -> int:
    """Публикует одну пачку неотправленных событий и возвращает число опубликованных.

    Строки блокируются `FOR UPDATE SKIP LOCKED`, поэтому несколько relay не берут одно и то же.
    Ошибка публикации откатывает пачку целиком: `published_at` не ставится ни у одного события,
    а следующий вызов опубликует их заново. Событие, ушедшее до ошибки, придёт второй раз —
    это допустимо, гарантия at-least-once, дедупликация лежит на consumer (ADR 0003).
    Событие с неизвестным `event_type` логируется и пропускается без `published_at`: оно
    остаётся в outbox для разбора и не блокирует остальные.
    """
    stmt = (
        select(OutboxEvent)
        .where(OutboxEvent.published_at.is_(None))
        .order_by(OutboxEvent.id)
        .limit(batch_size)
        .with_for_update(skip_locked=True)
    )
    events = (await session.execute(stmt)).scalars().all()
    published = 0
    try:
        for event in events:
            routing_key = ROUTING_KEYS.get(event.event_type)
            if routing_key is None:
                logger.error(
                    "outbox event %s has unknown event_type %r, skipped",
                    event.id,
                    event.event_type,
                )
                continue
            await publisher.publish(
                json.dumps(event.payload).encode(), routing_key, message_id=str(event.id)
            )
            event.published_at = datetime.now(UTC)
            published += 1
        await session.commit()
    except Exception:
        await session.rollback()
        raise
    return published


async def run_relay(
    sessionmaker: async_sessionmaker[AsyncSession], publisher: Publisher, interval: float
) -> None:
    """Крутит `relay_once` бесконечно; ошибки логирует и пробует снова через `interval`.

    После полностью опубликованной пачки следующая идёт сразу: в outbox, скорее всего, есть ещё.
    """
    batch_size = get_settings().outbox_batch_size
    while True:
        published = 0
        try:
            async with sessionmaker() as session:
                published = await relay_once(session, publisher, batch_size=batch_size)
        except Exception:
            logger.exception("outbox relay batch failed, will retry")
        if published < batch_size:
            await asyncio.sleep(interval)
