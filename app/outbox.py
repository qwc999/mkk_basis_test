import asyncio
import logging
from datetime import UTC, datetime

from aio_pika.exceptions import AMQPException, ChannelInvalidStateError
from faststream.rabbit import RabbitBroker
from pamqp.commands import Basic
from sqlalchemy import select
from sqlalchemy.exc import SQLAlchemyError
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker

from app.config import WorkerSettings
from app.messaging import PAYMENTS_EXCHANGE, PAYMENTS_QUEUE
from app.models import Outbox

logger = logging.getLogger(__name__)


class PublicationNotConfirmed(Exception):
    pass


async def publish_one(
    sessions: async_sessionmaker[AsyncSession], broker: RabbitBroker, publish_timeout: float
) -> bool:
    async with sessions.begin() as session:
        event = await session.scalar(
            select(Outbox)
            .where(Outbox.event_type == "payments.new", Outbox.published_at.is_(None))
            .order_by(Outbox.created_at, Outbox.id)
            .limit(1)
            .with_for_update(skip_locked=True)
        )
        if event is None:
            return False
        async with asyncio.timeout(publish_timeout):
            confirmation = await broker.publish(
                event.payload,
                exchange=PAYMENTS_EXCHANGE,
                routing_key=PAYMENTS_QUEUE.routing_key,
                persist=True,
                mandatory=True,
                timeout=publish_timeout,
                message_id=str(event.id),
            )
        if not isinstance(confirmation, Basic.Ack):
            raise PublicationNotConfirmed
        event.published_at = datetime.now(UTC)
    logger.info("Outbox event published: event_id=%s", event.id)
    return True


async def publish_outbox(sessions, broker: RabbitBroker, settings: WorkerSettings) -> None:
    while True:
        try:
            if await publish_one(sessions, broker, settings.outbox_publish_timeout):
                continue
        except (
            SQLAlchemyError,
            AMQPException,
            ChannelInvalidStateError,
            OSError,
            TimeoutError,
            PublicationNotConfirmed,
        ) as exc:
            logger.warning("Outbox publication failed (%s); will retry", type(exc).__name__)
            await asyncio.sleep(max(1.0, settings.outbox_poll_interval))
            continue
        await asyncio.sleep(settings.outbox_poll_interval)
