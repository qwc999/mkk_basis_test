import logging

from sqlalchemy import select
from sqlalchemy.dialects.postgresql import insert
from sqlalchemy.ext.asyncio import AsyncSession

from app.models import Outbox, Payment
from app.schemas import PaymentCreate

logger = logging.getLogger(__name__)


class IdempotencyConflict(Exception):
    pass


async def create_payment(
    session: AsyncSession, data: PaymentCreate, idempotency_key: str
) -> Payment:
    async with session.begin():
        payment = await session.scalar(
            insert(Payment)
            .values(
                amount=data.amount,
                currency=data.currency,
                description=data.description,
                payment_metadata=data.metadata,
                webhook_url=str(data.webhook_url),
                idempotency_key=idempotency_key,
            )
            .on_conflict_do_nothing(constraint="uq_payments_idempotency_key")
            .returning(Payment)
        )
        if payment is None:
            # At READ COMMITTED this separate statement sees the winning insert
            # after ON CONFLICT has waited for the concurrent transaction.
            payment, metadata_matches = (
                await session.execute(
                    select(Payment, Payment.payment_metadata == data.metadata).where(
                        Payment.idempotency_key == idempotency_key
                    )
                )
            ).one()
            if not (
                payment.amount == data.amount
                and payment.currency == data.currency
                and payment.description == data.description
                and metadata_matches
                and payment.webhook_url == str(data.webhook_url)
            ):
                raise IdempotencyConflict
        else:
            session.add(Outbox(event_type="payments.new", payload={"payment_id": str(payment.id)}))
        # The context commits both rows, or rolls back both if flush/commit fails.
    logger.info("Payment accepted: payment_id=%s", payment.id)
    return payment
