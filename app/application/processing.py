import logging
from datetime import UTC, datetime
from uuid import UUID

from app.application.ports import PaymentGateway, UnitOfWorkFactory
from app.domain.errors import PaymentNotFound

logger = logging.getLogger(__name__)


async def process_payment(
    uows: UnitOfWorkFactory, payment_id: UUID, gateway: PaymentGateway
) -> None:
    async with uows() as uow:
        payment = await uow.payments.get(payment_id, for_update=True)
        if payment is None:
            raise PaymentNotFound
        if payment.status == "pending":
            result = await gateway.charge(payment)
            await uow.payments.save(payment.complete(result, datetime.now(UTC)))
        await uow.commit()
    logger.info("Payment processing committed: payment_id=%s", payment_id)
