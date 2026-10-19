from faststream.rabbit import Channel, ExchangeType, RabbitBroker, RabbitExchange, RabbitQueue

from app.config import WorkerSettings

PAYMENTS_EXCHANGE = RabbitExchange("payments", type=ExchangeType.DIRECT, durable=True)
PAYMENTS_QUEUE = RabbitQueue("payments.new", durable=True, routing_key="payments.new")
RETRY_EXCHANGE = RabbitExchange("payments.retry", type=ExchangeType.DIRECT, durable=True)
RETRY_QUEUES = {
    attempt: RabbitQueue(
        f"payments.retry.{delay}s",
        durable=True,
        routing_key=f"payments.retry.{delay}s",
        arguments={
            "x-message-ttl": delay * 1000,
            "x-dead-letter-exchange": PAYMENTS_EXCHANGE.name,
            "x-dead-letter-routing-key": PAYMENTS_QUEUE.routing_key,
        },
    )
    for attempt, delay in ((1, 1), (2, 2))
}
DLX_EXCHANGE = RabbitExchange("payments.dlx", type=ExchangeType.DIRECT, durable=True)
DLQ_QUEUE = RabbitQueue("payments.dlq", durable=True, routing_key="payments.dlq")


def create_broker(settings: WorkerSettings) -> RabbitBroker:
    return RabbitBroker(
        settings.rabbitmq_url.get_secret_value(),
        default_channel=Channel(prefetch_count=1, publisher_confirms=True, on_return_raises=True),
        timeout=5,
        reconnect_interval=2,
        graceful_timeout=10,
        logger=None,
    )


async def declare_topology(broker: RabbitBroker):
    main_queue = None
    for schema, queues in (
        (PAYMENTS_EXCHANGE, (PAYMENTS_QUEUE,)),
        (RETRY_EXCHANGE, tuple(RETRY_QUEUES.values())),
        (DLX_EXCHANGE, (DLQ_QUEUE,)),
    ):
        exchange = await broker.declare_exchange(schema)
        for queue_schema in queues:
            queue = await broker.declare_queue(queue_schema)
            await queue.bind(exchange, routing_key=queue_schema.routing_key)
            if queue_schema is PAYMENTS_QUEUE:
                main_queue = queue
    return main_queue
