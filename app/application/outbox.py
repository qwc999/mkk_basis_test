import asyncio
import json
import logging
from datetime import UTC, datetime

from app.application.errors import PublicationError, StorageError
from app.application.messages import Destination, EventMessage
from app.application.ports import EventPublisher, UnitOfWorkFactory

logger = logging.getLogger(__name__)


async def publish_one(uows: UnitOfWorkFactory, publisher: EventPublisher) -> bool:
    async with uows() as uow:
        event = await uow.outbox.next_unpublished()
        if event is None:
            return False
        await publisher.publish(
            EventMessage(json.dumps(event.payload).encode(), str(event.id)), Destination.NEW
        )
        await uow.outbox.mark_published(event.id, datetime.now(UTC))
        await uow.commit()
    logger.info("Outbox event published: event_id=%s", event.id)
    return True


async def publish_outbox(
    uows: UnitOfWorkFactory, publisher: EventPublisher, poll_interval: float
) -> None:
    while True:
        try:
            if await publish_one(uows, publisher):
                continue
        except (StorageError, PublicationError) as exc:
            logger.warning("Outbox publication failed (%s); will retry", type(exc).__name__)
            await asyncio.sleep(max(1.0, poll_interval))
            continue
        await asyncio.sleep(poll_interval)
