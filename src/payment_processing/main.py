"""Точка входа API: `uvicorn payment_processing.main:app`."""

import time

from fastapi import APIRouter, Depends, FastAPI, Response
from prometheus_client import CONTENT_TYPE_LATEST, generate_latest
from starlette.types import ASGIApp, Message, Receive, Scope, Send

from payment_processing import metrics
from payment_processing.auth import require_api_key
from payment_processing.payments.router import router as payments_router


def route_template(scope: Scope) -> str:
    """Путь запроса с параметрами обратно в фигурных скобках: `/payments/{payment_id}`.

    `scope["route"].path` не годится: у маршрута из вложенного роутера там путь без префиксов
    внешних роутеров.
    """
    path: str = scope["path"]
    for name, value in scope.get("path_params", {}).items():
        path = path.replace(f"/{value}", f"/{{{name}}}")
    return path


class MetricsMiddleware:
    """Считает запросы и их длительность по шаблону маршрута (`/payments/{payment_id}`)."""

    def __init__(self, app: ASGIApp) -> None:
        self.app = app

    async def __call__(self, scope: Scope, receive: Receive, send: Send) -> None:
        if scope["type"] != "http":
            await self.app(scope, receive, send)
            return
        status = 500
        started = time.perf_counter()

        async def send_wrapper(message: Message) -> None:
            nonlocal status
            if message["type"] == "http.response.start":
                status = message["status"]
            await send(message)

        try:
            await self.app(scope, receive, send_wrapper)
        finally:
            # Роутер кладёт найденный маршрут в scope; без него путь — не метка, а мусор сканеров.
            if "route" in scope and scope["path"] != "/metrics":
                method, route = scope["method"], route_template(scope)
                metrics.HTTP_REQUESTS.labels(method, route, str(status)).inc()
                metrics.HTTP_DURATION.labels(method, route).observe(time.perf_counter() - started)


def create_app() -> FastAPI:
    app = FastAPI(title="Payment Processing")
    api = APIRouter(prefix="/api/v1", dependencies=[Depends(require_api_key)])

    @api.get("/health")
    async def health() -> dict[str, str]:
        return {"status": "ok"}

    api.include_router(payments_router)
    app.include_router(api)

    # Без ключа: метрики читает Prometheus. Наружу их закрывает сеть: в Kubernetes — отдельный
    # Service без Ingress, в compose порт API публикуется только для локального запуска.
    @app.get("/metrics", include_in_schema=False)
    def prometheus_metrics() -> Response:
        return Response(generate_latest(), media_type=CONTENT_TYPE_LATEST)

    app.add_middleware(MetricsMiddleware)
    return app


app = create_app()
