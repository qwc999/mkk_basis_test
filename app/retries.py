import asyncio
import logging
from typing import Literal

from faststream.rabbit import RabbitBroker
from pamqp.commands import Basic

from app.messaging import DLQ_QUEUE, DLX_EXCHANGE, RETRY_EXCHANGE, RETRY_QUEUES
from app.outbox import PublicationNotConfirmed

logger = logging.getLogger(__name__)
ErrorCategory = Literal["invalid_message", "payment_not_found", "processing_error", "webhook_error"]


async def retry_or_dead_letter(
    message,
    broker: RabbitBroker,
    *,
    attempt: int | None,
    reason: ErrorCategory,
    publish_timeout: float = 5,
) -> None:
    # Copy only controlled headers: malformed input and exception text cannot leak here.
    headers = {}
    if attempt is None or attempt == 3 or reason in {"invalid_message", "payment_not_found"}:
        exchange, queue = DLX_EXCHANGE, DLQ_QUEUE
        headers["x-error-category"] = reason
        if attempt is not None:
            headers["x-attempt"] = attempt
    else:
        exchange, queue = RETRY_EXCHANGE, RETRY_QUEUES[attempt]
        headers["x-attempt"] = attempt + 1
    async with asyncio.timeout(publish_timeout):
        confirmation = await broker.publish(
            message.body,
            exchange=exchange,
            routing_key=queue.routing_key,
            headers=headers,
            content_type="application/json",
            message_id=message.message_id,
            persist=True,
            mandatory=True,
            timeout=publish_timeout,
        )
    if not isinstance(confirmation, Basic.Ack):
        raise PublicationNotConfirmed
    # A failed transfer leaves the original unacknowledged for worker recovery.
    await message.ack()
    logger.warning("Payment event transferred: destination=%s reason=%s", queue.name, reason)
