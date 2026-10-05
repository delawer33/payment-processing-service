# Payment Processing

Асинхронный сервис процессинга платежей: приём платежей по API, обработка через платёжный шлюз в consumer, webhook-уведомления, outbox и DLQ

## Запуск

```bash
cp .env.example .env
uv sync
uv run uvicorn payment_processing.main:app --reload --port 8000
# или: docker compose up --build
curl localhost:8000/api/v1/health
```

## Разработка

```bash
uv run pre-commit install
uv run pytest
uv run ruff check . && uv run ruff format --check . && uv run mypy
```

Заметки для агентов — в `CLAUDE.md`; словарь домена — в `CONTEXT.md`; решения — в `docs/adr/`.
