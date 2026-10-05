---
status: accepted
date: 2026-10-05
---

# Один образ, два процесса; миграции применяет одноразовый сервис compose

API и consumer делят модели, конфиг и сессии БД, но живут в разных процессах: у API — event loop
uvicorn, у consumer — FastStream. Собирать два образа — дублировать слои ради разных `CMD`.
Применять миграции на старте каждого процесса — гонка двух `alembic upgrade head` при
одновременном запуске api и consumer.

Решение: один Dockerfile; `api` запускает uvicorn, `consumer` — тот же образ с
`command: faststream run`. Миграции применяет одноразовый сервис `migrate`
(`alembic upgrade head`), от которого `api` и `consumer` зависят через
`condition: service_completed_successfully`. В Kubernetes это стало бы Job или init-container.

## Consequences

- Образ собирается один раз; `docker compose up` поднимает всё, включая схему.
- Приложение никогда не создаёт схему само (`create_all` запрещён).
- Масштабирование consumer'а — `--scale consumer=N`; relay с `SKIP LOCKED` это выдерживает.
