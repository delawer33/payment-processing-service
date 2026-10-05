# Payment Processing

Асинхронный сервис процессинга платежей: приём платежей по API, обработка через платёжный шлюз в consumer, webhook-уведомления, outbox и DLQ

Глоссарий: `CONTEXT.md` — его словами пишутся код, тесты и доки. Решения: `docs/adr/` — перед
изменением области читай ADR, которые её касаются; пиши новый, когда делаешь выбор, который
компетентный инженер мог бы сделать иначе.

## Команды

```bash
uv sync                          # зависимости + venv (dev-группа включена)
uv run pytest                    # тесты; один: uv run pytest tests/test_x.py -k name
uv run ruff check . && uv run ruff format --check .
uv run mypy                      # strict; конфиг в pyproject.toml
uv run uvicorn payment_processing.main:app --reload --port 8000   # или preview "api" из .claude/launch.json
uv run faststream run payment_processing.consumer.app:app         # consumer, второй процесс
uv run pre-commit install        # один раз на клон
docker compose up --build        # нужен .env (cp .env.example .env); --profile demo добавляет webhook-sink
```

## Структура

```
src/payment_processing/        package by feature: один каталог на доменный срез
  main.py               create_app(); роутеры монтируются здесь под /api/v1
  config.py             Settings (pydantic-settings), всё через env
  db.py                 engine, sessionmaker, Base
  <feature>/            router.py, service.py, schemas.py, models.py — только то, что нужно срезу
  consumer/app.py       FastStream-приложение: подписчики RabbitMQ (отдельный процесс, тот же образ)
tests/                  pytest; API — по ASGI (httpx.AsyncClient); Postgres/RabbitMQ — testcontainers
migrations/             изменения схемы (Alembic); create_all в коде приложения не бывает
docs/adr/               решения
```

## Конвенции

- Сервисы — функции, принимающие зависимости явно; классы — только при реальном состоянии или
  полиморфизме. Абстракции (Protocol/ABC) — только там, где вторая реализация есть или предрешена.
- Два процесса из одного образа: `api` (uvicorn) и `consumer` (faststream). Код общий, точки входа разные.
- Деньги — только `Decimal` и `NUMERIC`; `float` для сумм запрещён.
- Конфигурация только через env (`config.py`); `.env` не коммитится, `.env.example` — всегда.
- Ожидаемые ошибки — типизированное доменное исключение с сообщением, по которому пользователь
  может действовать; `except Exception` не ловить.
- `pathlib.Path`, не `os.path`; `X | None`; фичи Python 3.13 использовать можно.
- Код пишется сразу под конфиг ruff (line-length 100), чтобы формат на pre-commit был no-op.
- Conventional commits (`feat:`, `fix:`, `chore:`, ...). Trunk-based, короткие ветки, зелёный CI
  перед мержем.
