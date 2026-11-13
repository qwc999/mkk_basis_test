import logging
from datetime import UTC, datetime
from uuid import UUID, uuid4

from app.application.ports import UnitOfWorkFactory
from app.domain.errors import IdempotencyConflict, PaymentNotFound
from app.domain.models import OutboxEvent, Payment, PaymentDraft

logger = logging.getLogger(__name__)


async def create_payment(
    uows: UnitOfWorkFactory, draft: PaymentDraft, idempotency_key: str
) -> Payment:
    async with uows() as uow:
        payment = await uow.payments.add_if_absent(draft, idempotency_key)
        if payment is None:
            payment = await uow.payments.get_by_key(idempotency_key)
            if payment is None or not payment.matches(draft):
                raise IdempotencyConflict
        else:
            await uow.outbox.add(
                OutboxEvent(
                    id=uuid4(),
                    event_type="payments.new",
                    payload={"payment_id": str(payment.id)},
                    created_at=datetime.now(UTC),
                )
            )
        await uow.commit()
    logger.info("Payment accepted: payment_id=%s", payment.id)
    return payment


async def get_payment(uows: UnitOfWorkFactory, payment_id: UUID) -> Payment:
    async with uows() as uow:
        payment = await uow.payments.get(payment_id)
        if payment is None:
            raise PaymentNotFound
        return payment
