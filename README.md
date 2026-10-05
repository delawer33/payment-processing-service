# Payment Processing

Асинхронный сервис процессинга платежей: приём платежей по API, обработка через платёжный шлюз
в consumer, webhook-уведомления, outbox и DLQ.

Клиент создаёт платёж запросом `POST /api/v1/payments` с заголовком `Idempotency-Key` и сразу
получает `202`. Платёж проходит через outbox и RabbitMQ в consumer, тот обращается к шлюзу,
фиксирует итоговый статус (`succeeded` или `failed`) и шлёт подписанный webhook на `webhook_url`.

## Запуск

```bash
cp .env.example .env
docker compose up --build
```

Поднимаются `db` (Postgres), `rabbitmq`, одноразовый `migrate` (применяет схему и завершается),
затем `api` (порт `8000`) и `consumer`. Все ручки под `/api/v1` требуют заголовок `X-API-Key`
со значением `API_KEY` из `.env`.

```bash
K="X-API-Key: change-me"   # значение API_KEY из .env
curl -i localhost:8000/api/v1/health
# HTTP/1.1 401 Unauthorized
# {"detail":"invalid API key"}

curl -i -H "$K" localhost:8000/api/v1/health
# HTTP/1.1 200 OK
# {"status":"ok"}
```

Для демо с приёмником webhook добавляется profile `demo`:

```bash
docker compose --profile demo up --build -d --wait
```

Сервис `webhook-sink` (`scripts/webhook_sink.py`) слушает порт 9000 внутри сети compose, печатает
каждый входящий POST в stdout и отвечает 200. Если порты 5432, 5672, 8000 на хосте заняты,
поменяйте `*_PORT_EXTERNAL` в `.env`.

## Переменные окружения

| Переменная | Назначение | По умолчанию |
|---|---|---|
| `API_KEY` | ключ клиентов в `X-API-Key`; без значения сервис не стартует | нет (в `.env.example`: `change-me`) |
| `WEBHOOK_SECRET` | ключ HMAC-SHA256 подписи webhook (`X-Signature: sha256=<hex>`) | `change-me` |
| `DATABASE_URL` | DSN Postgres (`postgresql+asyncpg://...`); в compose собирается из `POSTGRES_*` | нет |
| `RABBITMQ_URL` | URL RabbitMQ; в compose собирается из `RABBITMQ_*` | нет |
| `ENVIRONMENT`, `LOG_LEVEL` | окружение (`local`, `test`, `prod`) и уровень логов | `local`, `INFO` |
| `OUTBOX_POLL_INTERVAL`, `OUTBOX_BATCH_SIZE` | период опроса outbox (с) и размер пачки relay | `0.5`, `100` |
| `GATEWAY_DELAY_MIN`, `GATEWAY_DELAY_MAX` | диапазон задержки эмуляции шлюза, секунды | `2.0`, `5.0` |
| `GATEWAY_SUCCESS_RATE` | доля одобренных платежей в эмуляции шлюза | `0.9` |
| `WEBHOOK_TIMEOUT` | таймаут исходящего webhook, секунды | `5.0` |
| `RETRY_BASE_DELAY` | задержка перед первым повтором, секунды; каждая следующая вдвое больше | `1.0` |
| `CONSUMER_PREFETCH` | сколько сообщений consumer держит в работе одновременно | `10` |
| `WEBHOOK_SINK_FAIL_FIRST` | только demo: первые N запросов к `webhook-sink` получают 500 | `0` |
| `POSTGRES_*`, `RABBITMQ_*`, `API_PORT_*` | пользователи, пароли и порты контейнеров, см. `.env.example` | |

## Сценарии

Ниже вывод реального прогона `docker compose --profile demo up --build -d --wait`.
Дальше `K="X-API-Key: change-me"` и `B=localhost:8000/api/v1`.

### Создание платежа и webhook

```bash
curl -si -X POST $B/payments -H "$K" -H "Idempotency-Key: order-1001" \
  -H 'Content-Type: application/json' \
  -d '{"amount":"1500.00","currency":"RUB","description":"Заказ 1001","metadata":{"order_id":"1001"},"webhook_url":"http://webhook-sink:9000/hook"}'
```

```
HTTP/1.1 202 Accepted
content-type: application/json

{"payment_id":"0e31b515-703a-42c7-8101-5736a2950468","status":"pending","created_at":"2026-10-05T13:02:45.355084Z"}
```

Через ~6 секунд (задержка шлюза 2–5 с плюс опрос outbox):

```bash
curl -s $B/payments/0e31b515-703a-42c7-8101-5736a2950468 -H "$K"
```

```json
{"id":"0e31b515-703a-42c7-8101-5736a2950468","amount":"1500.00","currency":"RUB","description":"Заказ 1001","metadata":{"order_id":"1001"},"status":"succeeded","idempotency_key":"order-1001","webhook_url":"http://webhook-sink:9000/hook","created_at":"2026-10-05T13:02:45.355084Z","processed_at":"2026-10-05T13:03:36.161488Z","webhook_delivered_at":"2026-10-05T13:03:36.190432Z","gateway_reason":null}
```

В эмуляции шлюза 10% платежей отклоняются: тогда `status` равен `failed`, а `gateway_reason` —
`declined by issuer`. Webhook уходит в обоих случаях.

```bash
docker compose logs webhook-sink --no-log-prefix
```

```
webhook sink on :9000, fail first 0
#1 POST /hook -> 200 X-Signature=sha256=66fbbdc90d918c44f7eb66973c236c8482fea9ab8fcd9265d8426be737187849 body={"payment_id":"0e31b515-703a-42c7-8101-5736a2950468","status":"succeeded","amount":"1500.00","currency":"RUB","processed_at":"2026-10-05T13:03:36.161488+00:00","metadata":{"order_id":"1001"}}
```

Подпись — `HMAC-SHA256(WEBHOOK_SECRET, тело)`; получатель пересчитывает её по сырому телу запроса.

### Идемпотентность

Повтор с тем же `Idempotency-Key` и тем же телом возвращает тот же платёж и не создаёт нового:

```bash
curl -s -X POST $B/payments -H "$K" -H "Idempotency-Key: order-1001" \
  -H 'Content-Type: application/json' \
  -d '{"amount":"1500.00","currency":"RUB","description":"Заказ 1001","metadata":{"order_id":"1001"},"webhook_url":"http://webhook-sink:9000/hook"}'
# {"payment_id":"0e31b515-703a-42c7-8101-5736a2950468","status":"pending","created_at":"2026-10-05T13:02:45.355084Z"}
```

Тот же ключ с другим телом — `409`:

```bash
curl -si -X POST $B/payments -H "$K" -H "Idempotency-Key: order-1001" \
  -H 'Content-Type: application/json' \
  -d '{"amount":"99.00","currency":"RUB","webhook_url":"http://webhook-sink:9000/hook"}'
```

```
HTTP/1.1 409 Conflict
content-type: application/json

{"detail":"idempotency key 'order-1001' was already used with a different request body"}
```

### Повторы доставки webhook

С `WEBHOOK_SINK_FAIL_FIRST=2` приёмник отвечает 500 на первые два запроса. Consumer повторяет
сообщение через очередь `payments.retry` с задержками 1 с и 2 с; статус платежа к этому моменту
уже зафиксирован, шлюз повторно не вызывается.

```bash
WEBHOOK_SINK_FAIL_FIRST=2 docker compose --profile demo up -d --wait --force-recreate webhook-sink
# затем POST платежа, как выше
docker compose logs consumer --no-log-prefix | grep -E "retry in|delivered|acked"
```

```
2026-10-05 13:05:33,917 INFO payment_processing.consumer.app - payment 54961686-3a27-4e6b-abbf-ca90e8c4ceb5 attempt 0: retry in 1.0s
2026-10-05 13:05:34,936 INFO payment_processing.consumer.app - payment 54961686-3a27-4e6b-abbf-ca90e8c4ceb5 attempt 1: retry in 2.0s
2026-10-05 13:05:36,947 INFO payment_processing.consumer.handler - payment 54961686-3a27-4e6b-abbf-ca90e8c4ceb5 webhook delivered
2026-10-05 13:05:36,951 INFO payment_processing.consumer.app - payment 54961686-3a27-4e6b-abbf-ca90e8c4ceb5 attempt 2: acked
```

Приёмник при этом видит три запроса: `#1 ... -> 500`, `#2 ... -> 500`, `#3 ... -> 200`.

### DLQ

С `WEBHOOK_SINK_FAIL_FIRST=10` все три попытки (`MAX_ATTEMPTS = 3`) получают 500, и сообщение
уходит в `payments.dlq`:

```
2026-10-05 13:06:12,356 ERROR payment_processing.consumer.app - payment fabf4efe-f1a3-4f80-a497-ec0ffa619d8f attempt 2: sent to dlq
```

```bash
docker compose exec rabbitmq rabbitmqctl list_queues name messages
```

```
name	messages
payments.new	0
payments.retry	0
payments.dlq	1
```

Платёж в БД при этом уже `succeeded` или `failed`, не доставлен только webhook
(`webhook_delivered_at` равен `null`). Разбор DLQ ручной, см. «Сознательные упрощения».

## Тесты

```bash
uv sync
uv run pytest                      # 37 тестов
uv run ruff check . && uv run ruff format --check . && uv run mypy
```

Тестам нужен Docker: Postgres поднимается через testcontainers, RabbitMQ в тестах — in-memory
`TestRabbitBroker`. Миграции в тестах применяет тот же `alembic upgrade head`.

## Как это устроено

Подробности и схема потоков — в [ARCHITECTURE.md](ARCHITECTURE.md), решения — в `docs/adr/`,
словарь — в `CONTEXT.md`.

- API пишет платёж и событие outbox одной транзакцией, в брокер не ходит
  ([ADR 0001](docs/adr/0001-outbox-relay-at-least-once.md)).
- Идемпотентность держит `UNIQUE` на `idempotency_key`, а не проверка в коде
  ([ADR 0002](docs/adr/0002-idempotency-key-unique-constraint.md)).
- Consumer фиксирует терминальный статус отдельной транзакцией до webhook
  ([ADR 0003](docs/adr/0003-consumer-terminal-status-before-webhook.md)).
- Повторы идут через TTL-очередь `payments.retry`, исчерпавшие попытки уходят в `payments.dlq`
  ([ADR 0004](docs/adr/0004-retry-via-ttl-queue-and-dlq.md)).
- Один образ, два процесса; схему применяет сервис `migrate`
  ([ADR 0005](docs/adr/0005-one-image-two-processes-migrate-job.md)).

## Сознательные упрощения

- **Шлюз — эмуляция** со случайной задержкой и долей отказов, без таймаутов и circuit breaker.
  В проде у вызова шлюза был бы жёсткий таймаут, а при серии сбоев — размыкание цепи.
- **Один API-ключ** на весь сервис. В проде — ключи по клиентам с ротацией и привязкой платежей
  к клиенту.
- **DLQ разбирается вручную.** Сообщения из `payments.dlq` никто не читает; в проде нужны алерт
  на непустую очередь и инструмент переотправки.
- **Relay живёт в процессе consumer**, а не отдельным процессом. Несколько consumer безопасны
  (`SKIP LOCKED`), но публикация и обработка делят ресурсы. В проде relay выносится отдельно.
- **Опрос outbox вместо LISTEN/NOTIFY.** Простая схема с задержкой до `OUTBOX_POLL_INTERVAL`
  и постоянной лёгкой нагрузкой на БД. В проде — LISTEN/NOTIFY с опросом как страховкой.
- **Нет rate limit и метрик.** В проде — лимиты на клиента и метрики (глубина очередей, возраст
  непубликованных событий outbox, доля отказов шлюза, длина DLQ).
