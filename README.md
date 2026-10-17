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

На `/api/v1/payments` действует лимит `RATE_LIMIT_REQUESTS` запросов за `RATE_LIMIT_WINDOW`
секунд на API-ключ. Счётчик лежит в Redis и общий для всех реплик API
([ADR 0006](docs/adr/0006-rate-limit-fixed-window-redis-fail-open.md)). В пределах лимита
ответ несёт остаток квоты:

```
HTTP/1.1 202 Accepted
x-ratelimit-limit: 100
x-ratelimit-remaining: 99
```

Сверх лимита — `429` с `Retry-After` до конца текущего окна:

```
HTTP/1.1 429 Too Many Requests
retry-after: 1

{"detail":"rate limit of 100 requests per 1s exceeded"}
```

Если Redis недоступен, запрос проходит без лимита: в лог пишется предупреждение, растёт
`rate_limit_backend_errors_total`.

## Мониторинг

```bash
docker compose --profile monitoring up --build -d --wait
```

Добавляет Prometheus (`localhost:9090`) и Grafana (`localhost:3000`, без логина, только
чтение; admin — пароль `GRAFANA_ADMIN_PASSWORD`). Дашборд «Payment Processing» открывается
домашней страницей и собирается из `monitoring/`, ничего настраивать руками не нужно.

Prometheus собирает три источника:

| Источник | Адрес в сети compose | Что там |
|---|---|---|
| api | `api:8000/metrics` | запросы по маршруту и статусу, латентность, исход создания платежа (`created`, `replayed`, `conflict`), отказы rate limit и ошибки Redis |
| consumer | `consumer:9100/metrics` | терминальные статусы, латентность шлюза, доставка webhook, исход сообщений (`acked`, `retried`, `dead_lettered`, `requeued`), публикации outbox и возраст старейшего неопубликованного события |
| RabbitMQ | `rabbitmq:15692/metrics/per-object` | встроенный плагин `rabbitmq_prometheus`: глубина `payments.new`, `payments.retry`, `payments.dlq` |

`/metrics` API отдаётся без `X-API-Key` и наружу в проде не публикуется: его читает только
Prometheus изнутри кластера.

На дашборде три ряда. «Сводка» показывает приём в rps, p95 `POST /payments`, долю отказов шлюза,
глубину `payments.new`, DLQ и возраст outbox. «API» — маршруты, латентность, исходы и rate limit.
«Обработка» — шлюз, webhook, исходы сообщений, очереди и outbox.

## Kubernetes

Манифесты — `k8s/`, kustomize:

- `k8s/base/` — сам сервис: `ConfigMap`, Job `migrate`, Deployment `api` (2 реплики, `Service`,
  `HorizontalPodAutoscaler` 2–6 по CPU, `PodDisruptionBudget`) и Deployment `consumer` (2 реплики).
  Секреты берутся из Secret `payment-processing`.
- `k8s/overlays/kind/` — локальный стенд: namespace, Postgres, RabbitMQ и Redis внутри кластера,
  Secret из `secrets.env`.

```bash
docker compose build api
kind create cluster --name pps
kind load docker-image payment-processing:local --name pps
cp k8s/overlays/kind/secrets.env.example k8s/overlays/kind/secrets.env   # поменяйте change-me
kubectl apply -k k8s/overlays/kind
kubectl -n payment-processing wait --for=condition=complete job/migrate --timeout=180s
kubectl -n payment-processing port-forward svc/api 8000:80
```

Схему применяет Job `migrate` (ADR 0005). Поды `api` и `consumer` в init-контейнере ждут, пока
`alembic current` покажет `(head)`, поэтому порядок `apply` не важен. Job неизменяем, а тег образа
`:local` один на все сборки, поэтому новая версия выкатывается так: удалить Job
(`kubectl -n payment-processing delete job migrate`), `kubectl apply -k`, затем
`kubectl -n payment-processing rollout restart deploy/api deploy/consumer`. Откат приложения
после миграции требует совместимой назад схемы: под старого образа не узнает новую ревизию и
останется в init-контейнере с ошибкой alembic в логе.

Пулы соединений в ConfigMap посчитаны под `max_connections` Postgres (100): 6 подов `api` (максимум
HPA) по 5 + 5 и 2 `consumer` по 12. Больше реплик — пересчитать пулы или поставить PgBouncer.
`/metrics` API отдаётся без ключа на том же порту, что и API: Ingress перед ним должен
пропускать только `/api/`.
HPA нужен metrics-server; в kind его нет, без него автомасштаб не сработает, а остальное работает.

Для прода overlay `kind` заменяется своим: управляемые Postgres, RabbitMQ и Redis, Secret из
хранилища секретов, образ из registry.

Манифесты проверены `kubectl apply --dry-run=server` на кластере kind v0.31: API-сервер
принимает все объекты. Полный запуск подов в kind не прогонялся.

## Нагрузочный прогон

```bash
RATE_LIMIT_REQUESTS=1000000 docker compose up --build -d --wait   # лимит не мешает замеру
RATE=100 DURATION=60s docker compose --profile loadtest run --rm k6 run /scripts/payments.js
```

Сценарий `loadtest/payments.js` (k6, `constant-arrival-rate`) шлёт `POST /payments` с
постоянной частотой `RATE`. Ключи идемпотентности уникальны, 10% запросов повторяют
предыдущий ключ. Ожидаемые ответы — `202` и `429`. Замеряется приём, `webhook-sink` в прогоне не
нужен: недоставленные webhook уходят в повторы и DLQ, на приём это не влияет.

Стенд: ноутбук (i5-12500H, 16 потоков), compose целиком на нём же, API — один процесс uvicorn,
consumer — один процесс. Каждый прогон длится 60 с.

| Целевой rps | Принято, rps | Ошибки | p50 | p95 | p99 |
|---|---|---|---|---|---|
| 50 | 50.0 | 0 | 6 мс | 20 мс | 43 мс |
| 100 | 100.0 | 0 | 7 мс | 148 мс | 639 мс |
| 150 | 142.9 | 0, k6 не успел отправить 424 итерации | 345 мс | 4.1 с | 7.6 с |
| 250 | 184.2 | 0, k6 не успел отправить 2977 итераций | 4.1 с | 8.9 с | 11.1 с |

«Принято, rps» — `http_reqs` k6: завершённые запросы, делённые на полное время прогона. При
перегрузке это больше 60 с: k6 ещё до 30 с ждёт запросы в полёте (`gracefulStop`), поэтому
12 023 отправленных запроса при 250 rps дают 184.2/с, а не 200.

С лимитом по умолчанию (100 за 1 с) и нагрузкой 150 rps за 30 с принято 3003 запроса, ровно
100 в секунду. Ещё 1498 получили `429`. p50 — 4.7 мс, p95 — 196 мс.

Выводы:

- **Один процесс API держит около 140–180 `POST /payments` в секунду**, дальше растёт очередь и
  латентность. Упирается в CPU процесса. По профилю (py-spy) ~30% занимает ORM SQLAlchemy, ~24% —
  разрешение зависимостей FastAPI, ~9% — rate limit. Postgres не узкое место: pgbench на том же
  стенде дал ~11 тыс. tps. Масштабируется числом процессов: реплики в k8s, HPA по CPU.
- **Consumer обрабатывает ~2.8 платежа в секунду**, это prefetch 10, делённый на среднюю задержку
  эмуляции шлюза (2–5 с, медиана 3.6 с). За серию прогонов (включая повторные и прогон до
  правки дескрипторов) очередь `payments.new` выросла до ~36.5 тыс. сообщений, при этом возраст
  outbox оставался нулевым: relay успевает. Пропускная способность consumer задаётся шлюзом и
  масштабируется `CONSUMER_PREFETCH` и числом реплик.
- **Дескрипторы.** До `ulimits nofile` в compose при 250 rps 19.5% запросов падали с 500: под
  перегрузкой сокеты клиентов занимали лимит Docker в 1024 дескриптора, и новое соединение с
  БД получало «Too many open files». С лимитом 65536 ошибок нет.
- **Пул Redis.** Обычный пул redis-py сверх `max_connections` сразу бросает ошибку, и под
  нагрузкой rate limit молча отключался бы (fail open). Поэтому пул блокирующий, это покрыто
  тестом на 300 одновременных запросов.

Во время прогона дашборд Grafana показывает то же самое: приём в rps, p95, рост `payments.new`.

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
- **DLQ разбирается вручную.** Сообщения из `payments.dlq` никто не читает. Глубина DLQ видна на
  дашборде, но правил алертов нет; в проде нужны алерт на непустую очередь и инструмент
  переотправки.
- **Relay живёт в процессе consumer**, а не отдельным процессом. Несколько consumer безопасны
  (`SKIP LOCKED`), но публикация и обработка делят ресурсы. В проде relay выносится отдельно.
- **Опрос outbox вместо LISTEN/NOTIFY.** Простая схема с задержкой до `OUTBOX_POLL_INTERVAL`
  и постоянной лёгкой нагрузкой на БД; после полной пачки relay берёт следующую без паузы. В проде — LISTEN/NOTIFY с опросом как страховкой.
- **Liveness consumer проверяет только процесс.** `/metrics` отдаёт отдельный поток, и зависший
  event loop или потерянное соединение с брокером проба не видит; это видно по росту
  `payments.new` на дашборде. В проде — проба, которую обновляет сам цикл обработки.
- **Нет алертов и трейсинга.** Метрики и дашборд есть, Alertmanager и правила — нет; в проде —
  алерты на DLQ, возраст outbox, долю отказов шлюза и ошибки Redis, плюс трейсы OpenTelemetry.
