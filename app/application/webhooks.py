from datetime import UTC, datetime
from uuid import UUID

from app.application.payments import get_payment
from app.application.ports import UnitOfWorkFactory, WebhookSender
from app.domain.errors import PaymentNotFound


async def deliver_webhook(uows: UnitOfWorkFactory, payment_id: UUID, sender: WebhookSender) -> None:
    payment = await get_payment(uows, payment_id)
    if payment.webhook_delivered_at is not None:
        return
    payment.require_final_result()
    # The read UoW is closed before invoking the external dependency.
    await sender.send(payment)
    async with uows() as uow:
        current = await uow.payments.get(payment_id, for_update=True)
        if current is None:
            raise PaymentNotFound
        await uow.payments.save(current.delivered(datetime.now(UTC)))
        await uow.commit()
