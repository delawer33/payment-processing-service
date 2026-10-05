"""Топология RabbitMQ (ADR 0004): один direct-обменник и три очереди."""

from faststream.rabbit import ExchangeType, RabbitExchange, RabbitQueue

EXCHANGE = RabbitExchange("payments", type=ExchangeType.DIRECT, durable=True)

NEW_ROUTING_KEY = "payments.new"
RETRY_ROUTING_KEY = "payments.retry"
DLQ_ROUTING_KEY = "payments.dlq"

NEW_QUEUE = RabbitQueue(
    "payments.new",
    durable=True,
    routing_key=NEW_ROUTING_KEY,
    arguments={
        "x-dead-letter-exchange": EXCHANGE.name,
        "x-dead-letter-routing-key": DLQ_ROUTING_KEY,
    },
)
# Ожидание перед повтором: читателей нет, по истечении TTL сообщение уходит обратно в payments.new.
RETRY_QUEUE = RabbitQueue(
    "payments.retry",
    durable=True,
    routing_key=RETRY_ROUTING_KEY,
    arguments={
        "x-dead-letter-exchange": EXCHANGE.name,
        "x-dead-letter-routing-key": NEW_ROUTING_KEY,
    },
)
# Окончательно упавшие сообщения: читателей нет, разбор ручной.
DLQ_QUEUE = RabbitQueue("payments.dlq", durable=True, routing_key=DLQ_ROUTING_KEY)

ATTEMPT_HEADER = "x-attempt"
MAX_ATTEMPTS = 3
