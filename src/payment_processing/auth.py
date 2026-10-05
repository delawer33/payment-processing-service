"""Аутентификация клиентов по статическому ключу в заголовке `X-API-Key`."""

import secrets
from typing import Annotated

from fastapi import Depends, Header, HTTPException, status

from payment_processing.config import Settings, get_settings


def require_api_key(
    settings: Annotated[Settings, Depends(get_settings)],
    x_api_key: Annotated[str | None, Header()] = None,
) -> None:
    expected = settings.api_key.encode()
    received = (x_api_key or "").encode()
    if x_api_key is None or not secrets.compare_digest(received, expected):
        raise HTTPException(status.HTTP_401_UNAUTHORIZED, detail="invalid API key")
