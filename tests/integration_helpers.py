"""Wire the real adapters for the existing integration regression scenarios."""

from app.application.consumer import consume_payment as handle_payment
from app.application.outbox import publish_one as publish_event
from app.application.outbox import publish_outbox as run_outbox
from app.application.processing import process_payment as process
from app.application.retries import retry_or_dead_letter as transfer
from app.application.webhooks import deliver_webhook as deliver
from app.infrastructure.database.uow import create_uow_factory
from app.infrastructure.messaging import RabbitEventPublisher
from app.infrastructure.webhooks import HttpxWebhookSender


class FunctionGateway:
    def __init__(self, simulator):
        self.simulator = simulator

    async def charge(self, payment):
        return await self.simulator()


async def process_payment(sessions, payment_id, simulator):
    await process(create_uow_factory(sessions), payment_id, FunctionGateway(simulator))


async def deliver_webhook(sessions, payment_id, client):
    await deliver(create_uow_factory(sessions), payment_id, HttpxWebhookSender(client))


async def publish_one(sessions, broker, publish_timeout):
    return await publish_event(
        create_uow_factory(sessions), RabbitEventPublisher(broker, publish_timeout)
    )


async def publish_outbox(sessions, broker, settings):
    await run_outbox(
        create_uow_factory(sessions),
        RabbitEventPublisher(broker, settings.outbox_publish_timeout),
        settings.outbox_poll_interval,
    )


async def retry_or_dead_letter(message, broker, *, attempt, reason):
    await transfer(message, RabbitEventPublisher(broker, 5), attempt=attempt, reason=reason)


async def consume_payment(message, sessions, broker, client, simulator):
    await handle_payment(
        message,
        create_uow_factory(sessions),
        RabbitEventPublisher(broker, 5),
        HttpxWebhookSender(client),
        FunctionGateway(simulator),
    )
