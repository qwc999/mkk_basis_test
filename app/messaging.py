from faststream.rabbit import Channel, ExchangeType, RabbitBroker, RabbitExchange, RabbitQueue

from app.config import WorkerSettings

PAYMENTS_EXCHANGE = RabbitExchange("payments", type=ExchangeType.DIRECT, durable=True)
PAYMENTS_QUEUE = RabbitQueue("payments.new", durable=True, routing_key="payments.new")


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
    exchange = await broker.declare_exchange(PAYMENTS_EXCHANGE)
    queue = await broker.declare_queue(PAYMENTS_QUEUE)
    await queue.bind(exchange, routing_key=PAYMENTS_QUEUE.routing_key)
    return queue
