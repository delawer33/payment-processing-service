"""Метрики Prometheus процесса API, отдаются на `/metrics`.

Метрики consumer — в `consumer/metrics.py`: модуль регистрирует метрики при импорте, и общий
модуль заставил бы каждый процесс отдавать нулевые ряды чужих метрик.
Метки ограничены конечными множествами (шаблон маршрута, статус), чтобы число рядов не росло
вместе с трафиком.
"""

from prometheus_client import Counter, Histogram, disable_created_metrics

# Ряды `*_created` (время создания счётчика) дашборду не нужны и удваивают ответ `/metrics`.
disable_created_metrics()  # type: ignore[no-untyped-call]  # без аннотаций в prometheus_client

HTTP_REQUESTS = Counter("http_requests_total", "HTTP-запросы к API", ["method", "route", "status"])
HTTP_DURATION = Histogram(
    "http_request_duration_seconds",
    "Время обработки HTTP-запроса",
    ["method", "route"],
    buckets=(0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1.0, 2.5, 5.0),
)
PAYMENTS_ACCEPTED = Counter(
    "payments_accepted_total",
    "Ответы на создание платежа: created — новый, replayed — повтор ключа, conflict — 409",
    ["result"],
)
RATE_LIMIT_REJECTIONS = Counter("rate_limit_rejections_total", "Запросы, отбитые 429")
RATE_LIMIT_BACKEND_ERRORS = Counter(
    "rate_limit_backend_errors_total", "Запросы, пропущенные без лимита из-за недоступного Redis"
)
