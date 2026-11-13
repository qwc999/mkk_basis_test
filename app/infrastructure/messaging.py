import asyncio

from aio_pika.exceptions import AMQPException, ChannelInvalidStateError
from faststream.rabbit import Channel, ExchangeType, RabbitBroker, RabbitExchange, RabbitQueue
from pamqp.commands import Basic

from app.application.errors import PublicationError, PublicationNotConfirmed
from app.application.messages import Destination, EventMessage
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


class RabbitEventPublisher:
    def __init__(self, broker: RabbitBroker, publish_timeout: float):
        self.broker = broker
        self.publish_timeout = publish_timeout

    async def publish(self, message: EventMessage, destination: Destination) -> None:
        routes = {
            Destination.NEW: (PAYMENTS_EXCHANGE, PAYMENTS_QUEUE),
            Destination.RETRY_1S: (RETRY_EXCHANGE, RETRY_QUEUES[1]),
            Destination.RETRY_2S: (RETRY_EXCHANGE, RETRY_QUEUES[2]),
            Destination.DEAD_LETTER: (DLX_EXCHANGE, DLQ_QUEUE),
        }
        exchange, queue = routes[destination]
        try:
            async with asyncio.timeout(self.publish_timeout):
                confirmation = await self.broker.publish(
                    message.body,
                    exchange=exchange,
                    routing_key=queue.routing_key,
                    headers=message.headers,
                    content_type="application/json",
                    message_id=message.message_id,
                    persist=True,
                    mandatory=True,
                    timeout=self.publish_timeout,
                )
        except (AMQPException, ChannelInvalidStateError, OSError, TimeoutError) as exc:
            raise PublicationError from exc
        if not isinstance(confirmation, Basic.Ack):
            raise PublicationNotConfirmed
