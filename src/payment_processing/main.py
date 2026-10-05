"""Точка входа API: `uvicorn payment_processing.main:app`."""

from fastapi import APIRouter, Depends, FastAPI

from payment_processing.auth import require_api_key
from payment_processing.payments.router import router as payments_router


def create_app() -> FastAPI:
    app = FastAPI(title="Payment Processing")
    api = APIRouter(prefix="/api/v1", dependencies=[Depends(require_api_key)])

    @api.get("/health")
    async def health() -> dict[str, str]:
        return {"status": "ok"}

    api.include_router(payments_router)
    app.include_router(api)
    return app


app = create_app()
