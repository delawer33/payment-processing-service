"""Метрики Prometheus процесса consumer (и relay в нём), отдаются на `CONSUMER_METRICS_PORT`."""

from prometheus_client import Counter, Gauge, Histogram, disable_created_metrics

# Ряды `*_created` (время создания счётчика) дашборду не нужны и удваивают ответ `/metrics`.
disable_created_metrics()  # type: ignore[no-untyped-call]  # без аннотаций в prometheus_client

PAYMENTS_FINALISED = Counter(
    "payments_finalised_total", "Платежи, доведённые до терминального статуса", ["status"]
)
GATEWAY_DURATION = Histogram(
    "gateway_charge_duration_seconds",
    "Время вызова шлюза",
    buckets=(0.1, 0.5, 1.0, 2.0, 3.0, 4.0, 5.0, 7.5, 10.0),
)
WEBHOOKS = Counter("webhook_deliveries_total", "Попытки доставки webhook", ["result"])
CONSUMER_MESSAGES = Counter(
    "consumer_messages_total",
    "Исход обработки сообщения: acked, retried, dead_lettered, requeued",
    ["outcome"],
)
OUTBOX_PUBLISHED = Counter("outbox_published_total", "События outbox, опубликованные relay")
OUTBOX_OLDEST_AGE = Gauge(
    "outbox_oldest_unpublished_age_seconds",
    "Возраст самого старого неопубликованного события outbox; 0 — outbox пуст",
)
