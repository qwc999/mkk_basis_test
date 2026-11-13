import json
from uuid import UUID

from app.application.ports import (
    EventPublisher,
    MessageDelivery,
    PaymentGateway,
    UnitOfWorkFactory,
    WebhookSender,
)
from app.application.processing import process_payment
from app.application.retries import retry_or_dead_letter
from app.application.webhooks import deliver_webhook
from app.domain.errors import PaymentNotFound


async def consume_payment(
    message: MessageDelivery,
    uows: UnitOfWorkFactory,
    publisher: EventPublisher,
    sender: WebhookSender,
    gateway: PaymentGateway,
) -> None:
    attempt = message.headers.get("x-attempt", 1)
    if type(attempt) is not int or not 1 <= attempt <= 3:
        await retry_or_dead_letter(message, publisher, attempt=None, reason="invalid_message")
        return
    try:
        data = json.loads(message.body)
        if not isinstance(data, dict) or set(data) != {"payment_id"}:
            raise ValueError
        if not isinstance(data["payment_id"], str):
            raise ValueError
        payment_id = UUID(data["payment_id"])
    except (ValueError, UnicodeError):
        await retry_or_dead_letter(message, publisher, attempt=attempt, reason="invalid_message")
        return
    reason = "processing_error"
    try:
        await process_payment(uows, payment_id, gateway)
        reason = "webhook_error"
        await deliver_webhook(uows, payment_id, sender)
    except PaymentNotFound:
        await retry_or_dead_letter(message, publisher, attempt=attempt, reason="payment_not_found")
        return
    except Exception:
        await retry_or_dead_letter(message, publisher, attempt=attempt, reason=reason)
        return
    await message.ack()
