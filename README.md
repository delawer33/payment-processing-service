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

Поднимаются `db` (Postgres), `rabbitmq`, `redis` (счётчики rate limit), одноразовый `migrate`
(применяет схему и завершается), затем `api` (порт `8000`) и `consumer`. Все ручки под `/api/v1` требуют заголовок `X-API-Key`
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
| `REDIS_URL` | URL Redis для rate limit; в compose `redis://redis:6379/0` | нет |
| `REDIS_TIMEOUT` | таймаут операции и ожидания соединения из пула Redis, секунды | `0.5` |
| `REDIS_MAX_CONNECTIONS` | размер пула соединений с Redis на процесс API | `50` |
| `RATE_LIMIT_REQUESTS`, `RATE_LIMIT_WINDOW` | лимит запросов к `/payments` на API-ключ за окно в секундах | `100`, `1` |
| `DB_POOL_SIZE`, `DB_MAX_OVERFLOW` | пул соединений SQLAlchemy на процесс | `20`, `10` |
| `CONSUMER_METRICS_PORT` | порт `/metrics` процесса consumer | `9100` |
| `WEBHOOK_SINK_FAIL_FIRST` | только demo: первые N запросов к `webhook-sink` получают 500 | `0` |
| `POSTGRES_*`, `RABBITMQ_*`, `API_PORT_*` | пользователи, пароли и порты контейнеров, см. `.env.example` | |
| `PROMETHEUS_PORT_EXTERNAL`, `GRAFANA_PORT_EXTERNAL`, `GRAFANA_ADMIN_PASSWORD` | только compose, profile `monitoring` | `9090`, `3000`, `change-me` |

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

### Rate limit

Лимит на `/api/v1/payments`: `RATE_LIMIT_REQUESTS` запросов за `RATE_LIMIT_WINDOW` секунд на
API-ключ. Счётчик лежит в Redis и общий для всех реплик API
([ADR 0006](docs/adr/0006-rate-limit-fixed-window-redis-fail-open.md)).

Успешный ответ показывает остаток квоты:

```
HTTP/1.1 202 Accepted
x-ratelimit-limit: 100
x-ratelimit-remaining: 99
```

Сверх лимита API отвечает `429`. `Retry-After` показывает, сколько секунд осталось до конца окна:

```
HTTP/1.1 429 Too Many Requests
retry-after: 1

{"detail":"rate limit of 100 requests per 1s exceeded"}
```

Если Redis недоступен, запрос проходит без лимита. В лог пишется предупреждение, растёт
`rate_limit_backend_errors_total`.

## Мониторинг

```bash
docker compose --profile monitoring up --build -d --wait
```

Grafana открывается на `localhost:3000` сразу с дашбордом «Payment Processing», без логина.
Prometheus слушает `localhost:9090`. Пароль admin в Grafana задаёт `GRAFANA_ADMIN_PASSWORD`.

Prometheus собирает три источника:

| Источник | Адрес | Метрики |
|---|---|---|
| api | `api:8000/metrics` | запросы, латентность, исход создания платежа, rate limit |
| consumer | `consumer:9100/metrics` | статусы платежей, шлюз, webhook, исход сообщений, outbox |
| RabbitMQ | `rabbitmq:15692/metrics/per-object` | глубина очередей `payments.*` |

На дашборде три ряда:

- «Сводка»: приём, p95, отказы шлюза, `payments.new`, DLQ, возраст outbox.
- «API»: маршруты, латентность, исходы создания платежа, rate limit.
- «Обработка»: шлюз, webhook, сообщения consumer, очереди, outbox.

`/metrics` API отдаётся без `X-API-Key`, поэтому наружу его не публикуют.

## Kubernetes

Манифесты лежат в `k8s/`, собираются kustomize:

- `k8s/base/`: сервис. ConfigMap, Job `migrate`, Deployment `api` и `consumer`.
- `k8s/overlays/kind/`: локальный стенд. Postgres, RabbitMQ, Redis и Secret из `secrets.env`.

Запуск в kind:

```bash
docker compose build api
kind create cluster --name pps
kind load docker-image payment-processing:local --name pps
cp k8s/overlays/kind/secrets.env.example k8s/overlays/kind/secrets.env   # поменяйте change-me
kubectl apply -k k8s/overlays/kind
kubectl -n payment-processing wait --for=condition=complete job/migrate --timeout=180s
kubectl -n payment-processing port-forward svc/api 8000:80
```

Порядок `apply` не важен. Схему применяет Job `migrate` (ADR 0005), поды ждут её в
init-контейнере.

Новая версия выкатывается тремя командами:

```bash
kubectl -n payment-processing delete job migrate
kubectl apply -k k8s/overlays/kind
kubectl -n payment-processing rollout restart deploy/api deploy/consumer
```

Job удаляют, потому что он неизменяем. Restart нужен, потому что тег `:local` один на все сборки.

Откат приложения после миграции работает, только если схема совместима назад. Иначе под старого
образа останется в init-контейнере с ошибкой alembic в логе.

У `api` есть автомасштаб (HPA) от 2 до 6 подов по CPU. Ему нужен metrics-server, в kind его нет.

Пулы соединений рассчитаны на `max_connections` 100 у Postgres: 6 подов `api` по 10 и 2
`consumer` по 12. Если нужно больше реплик, пересчитайте пулы или поставьте PgBouncer.

`/metrics` API работает на том же порту, что и API. Ingress должен пропускать только `/api/`.

Для прода overlay `kind` заменяют своим: управляемые Postgres, RabbitMQ и Redis, Secret из
хранилища секретов, образ из registry.

Проверено в kind v0.31 на чистом кластере:

- Job и все поды поднимаются без перезапусков.
- Платёж доходит от API до `succeeded`.
- Лимит общий на два пода `api`.
- Под `api` останавливается по SIGTERM за ~2 с.

## Нагрузочный прогон

```bash
RATE_LIMIT_REQUESTS=1000000 docker compose up --build -d --wait   # лимит не мешает замеру
RATE=100 DURATION=60s docker compose --profile loadtest run --rm k6 run /scripts/payments.js
```

Сценарий `loadtest/payments.js` шлёт `POST /payments` с постоянной частотой `RATE`. 10% запросов
повторяют предыдущий ключ идемпотентности. Ответ `429` ошибкой не считается. Замеряется только
приём, `webhook-sink` не нужен.

Стенд: ноутбук i5-12500H, 16 потоков. На нём же весь compose, API и consumer по одному процессу.
Каждый прогон длится 60 с, ошибок нет ни в одном.

| Целевой rps | Принято, rps | k6 не успел отправить | p50 | p95 | p99 |
|---|---|---|---|---|---|
| 50 | 50.0 | 0 | 6 мс | 20 мс | 43 мс |
| 100 | 100.0 | 0 | 7 мс | 148 мс | 639 мс |
| 150 | 142.9 | 424 | 345 мс | 4.1 с | 7.6 с |
| 250 | 184.2 | 2977 | 4.1 с | 8.9 с | 11.1 с |

«Принято» делится на полное время прогона. При перегрузке k6 ещё до 30 с ждёт запросы в полёте.
Поэтому 12 023 запроса при 250 rps дают 184.2 в секунду, а не 200.

С лимитом по умолчанию (100 в секунду) прогон на 150 rps за 30 с: принято 3003 запроса, 1498
получили `429`. p50 4.7 мс, p95 196 мс.

### Выводы

Один процесс API выдерживает 140–180 `POST /payments` в секунду, дальше растут очередь и
латентность. Под большую нагрузку нужны дополнительные поды, их добавляет HPA.

Предел задаёт CPU процесса. По py-spy ~30% уходит на ORM SQLAlchemy, ~24% на зависимости FastAPI,
~9% на rate limit. Postgres не узкое место: pgbench на том же стенде дал ~11 тыс. tps.

Consumer обрабатывает ~2.8 платежа в секунду: prefetch 10, делённый на медианную задержку шлюза
3.6 с. Ускоряется через `CONSUMER_PREFETCH` и число реплик. Relay при этом успевает: возраст
outbox оставался нулевым, хотя `payments.new` за серию прогонов выросла до ~36.5 тыс.

Нагрузка нашла две ошибки, обе исправлены.

При 250 rps 19.5% запросов падали с «Too many open files»: лимит Docker в 1024 дескриптора.
В compose теперь `ulimits nofile` 65536, ошибок нет.

Обычный пул redis-py сверх `max_connections` бросает ошибку, и rate limit молча отключался бы.
Пул сделан блокирующим, это проверяет тест на 300 одновременных запросов.

## Тесты

```bash
uv sync
uv run pytest                      # 67 тестов
uv run ruff check . && uv run ruff format --check . && uv run mypy
```

Тестам нужен Docker: Postgres и Redis поднимаются через testcontainers, RabbitMQ в тестах —
in-memory `TestRabbitBroker`. Миграции в тестах применяет тот же `alembic upgrade head`.

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
- Rate limit — фиксированное окно в Redis на API-ключ; без Redis запрос проходит
  ([ADR 0006](docs/adr/0006-rate-limit-fixed-window-redis-fail-open.md)).

## Сознательные упрощения

- **Шлюз — эмуляция** со случайной задержкой и долей отказов, без таймаутов и circuit breaker.
  В проде у вызова шлюза был бы жёсткий таймаут, а при серии сбоев — размыкание цепи.
- **Один API-ключ** на весь сервис. В проде — ключи по клиентам с ротацией и привязкой платежей
  к клиенту.
- **DLQ разбирается вручную.** Сообщения из `payments.dlq` никто не читает. В проде нужны алерт на
  непустую очередь и инструмент переотправки.
- **Relay живёт в процессе consumer**, а не отдельным процессом. Несколько consumer безопасны
  (`SKIP LOCKED`), но публикация и обработка делят ресурсы. В проде relay выносится отдельно.
- **Опрос outbox вместо LISTEN/NOTIFY.** Простая схема с задержкой до `OUTBOX_POLL_INTERVAL`
  и постоянной лёгкой нагрузкой на БД; после полной пачки relay берёт следующую без паузы. В проде — LISTEN/NOTIFY с опросом как страховкой.
- **Liveness consumer проверяет только процесс.** Зависший event loop или обрыв связи с брокером
  проба не видит, их выдаёт только рост `payments.new`.
- **Нет алертов и трейсинга.** В проде нужны алерты на DLQ, возраст outbox, отказы шлюза и ошибки
  Redis, плюс трейсы OpenTelemetry.
