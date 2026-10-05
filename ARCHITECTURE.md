# Архитектура

## Потоки

```
 клиент
   |  POST /api/v1/payments  (X-API-Key, Idempotency-Key)
   v
 [api]  одна транзакция:  INSERT payments (pending) + INSERT outbox
   |                      повтор ключа -> тот же платёж, другое тело -> 409
   |  202 {payment_id, status: pending}
   v
 [Postgres]  payments, outbox
   ^   |
   |   |  relay (в процессе consumer): SELECT ... FOR UPDATE SKIP LOCKED
   |   |  publish с подтверждением брокера -> published_at
   |   v
   |  [RabbitMQ]  exchange `payments` (direct)
   |      |
   |      |  payments.new
   |      v
   |  [consumer]  process_payment
   |      1. транзакция A: FOR UPDATE, pending -> шлюз -> succeeded|failed, commit
   |      2. транзакция B: FOR UPDATE, webhook_delivered_at is null -> POST webhook_url
   |      |                                        |
   +------+ статус и webhook_delivered_at          +--> [получатель webhook]
          |
          |  исключение (шлюз недоступен, webhook не 2xx)
          v
   попытка < 3:  publish копии в payments.retry (TTL 1 с, затем 2 с), ack оригинала
                 по истечении TTL dead-letter -> payments.new (заголовок x-attempt + 1)
   попытка = 3:  reject(requeue=False) -> dead-letter -> payments.dlq (ручной разбор)
```

Очереди `payments.retry` и `payments.dlq` читателей не имеют. Привязки к обменнику `payments`
объявляет consumer при старте.

## Карта модулей

`src/payment_processing/`

| Файл | Роль |
|---|---|
| `main.py` | `create_app()`: монтирует роутеры под `/api/v1` с проверкой ключа |
| `auth.py` | зависимость `require_api_key`: сравнение `X-API-Key` за постоянное время |
| `config.py` | `Settings`, вся конфигурация из env |
| `db.py` | engine, sessionmaker, `Base`, зависимость `get_session` |
| `gateway.py` | `PaymentGateway` и `EmulatedGateway` (задержка, доля отказов) |
| `webhooks.py` | подпись HMAC, тело webhook, `deliver`, `WebhookDeliveryError` |
| `payments/models.py` | модель `Payment`, enum `PaymentStatus`, CHECK и UNIQUE |
| `payments/schemas.py` | контракт API: `PaymentCreate`, `PaymentAccepted`, `PaymentDetails` |
| `payments/service.py` | `create_payment` (идемпотентность, outbox), `get_payment` |
| `payments/router.py` | `POST /payments`, `GET /payments/{id}` |
| `outbox/models.py` | модель `OutboxEvent` |
| `outbox/relay.py` | `relay_once`, `run_relay`: перенос outbox в брокер |
| `messaging/schemas.py` | сообщение `PaymentCreated` |
| `messaging/topology.py` | обменник, три очереди, `MAX_ATTEMPTS`, заголовок попытки |
| `consumer/handler.py` | `process_payment`: шлюз, фиксация статуса, webhook |
| `consumer/app.py` | FastStream-приложение: подписчик, retry/DLQ, запуск relay |

Вне пакета: `migrations/versions/0001_payments_outbox.py` (схема, триггер),
`scripts/webhook_sink.py` (приёмник webhook для демо), `docker-compose.yml`, `Dockerfile`.

## Инварианты

| Инвариант | Кто обеспечивает |
|---|---|
| Один `Idempotency-Key` — один платёж | `UNIQUE uq_payments_idempotency_key` + `INSERT ... ON CONFLICT DO NOTHING` (ADR 0002) |
| Тот же ключ с другим телом не проходит | `request_hash` платежа сравнивается с хешем запроса, иначе `IdempotencyConflict` и 409 |
| Сумма положительна, валюта из списка | `CHECK ck_payments_amount_positive`, `ck_payments_currency`; деньги `NUMERIC(18,2)` и `Decimal` |
| Терминальный статус не меняется | триггер `payments_status_terminal_guard` (ADR 0003) |
| Платёж и событие о нём возникают вместе | одна транзакция API: `payments` + `outbox` (ADR 0001) |
| Событие не теряется при недоступном брокере | `published_at` ставится только после подтверждения брокера; at-least-once |
| Несколько relay не берут одно событие | `FOR UPDATE SKIP LOCKED` в `relay_once` |
| Дубль сообщения не вызывает шлюз дважды и не шлёт второй webhook | `FOR UPDATE` на строке платежа, проверка `pending` и `webhook_delivered_at` (ADR 0003) |
| Сбой webhook не откатывает статус | статус фиксируется отдельной транзакцией до доставки |
| Исчерпавшее попытки сообщение не теряется | `x-dead-letter-exchange` у `payments.new` -> `payments.dlq` (ADR 0004) |
| Схему создаёт только миграция | сервис `migrate` (`alembic upgrade head`), `create_all` в коде нет (ADR 0005) |
