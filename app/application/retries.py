import logging

from app.application.messages import Destination, ErrorCategory, EventMessage
from app.application.ports import EventPublisher, MessageDelivery

logger = logging.getLogger(__name__)


async def retry_or_dead_letter(
    message: MessageDelivery,
    publisher: EventPublisher,
    *,
    attempt: int | None,
    reason: ErrorCategory,
) -> None:
    headers: dict[str, object] = {}
    if attempt is None or attempt == 3 or reason in {"invalid_message", "payment_not_found"}:
        destination = Destination.DEAD_LETTER
        headers["x-error-category"] = reason
        if attempt is not None:
            headers["x-attempt"] = attempt
    else:
        destination = Destination.RETRY_1S if attempt == 1 else Destination.RETRY_2S
        headers["x-attempt"] = attempt + 1
    await publisher.publish(EventMessage(message.body, message.message_id, headers), destination)
    await message.ack()
    logger.warning("Payment event transferred: destination=%s reason=%s", destination, reason)
