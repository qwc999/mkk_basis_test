from datetime import datetime
from uuid import UUID

from sqlalchemy import select, update
from sqlalchemy.dialects.postgresql import insert
from sqlalchemy.ext.asyncio import AsyncSession

from app.domain.errors import PaymentNotFound
from app.domain.models import OutboxEvent, Payment, PaymentDraft
from app.infrastructure.database.models import OutboxRecord, PaymentRecord


def to_payment(row: PaymentRecord) -> Payment:
    return Payment(
        id=row.id,
        amount=row.amount,
        currency=row.currency,
        description=row.description,
        metadata=row.payment_metadata,
        webhook_url=row.webhook_url,
        idempotency_key=row.idempotency_key,
        created_at=row.created_at,
        status=row.status,
        processed_at=row.processed_at,
        webhook_delivered_at=row.webhook_delivered_at,
    )


class SqlAlchemyPaymentRepository:
    def __init__(self, session: AsyncSession):
        self.session = session

    async def add_if_absent(self, draft: PaymentDraft, key: str) -> Payment | None:
        row = await self.session.scalar(
            insert(PaymentRecord)
            .values(
                amount=draft.amount,
                currency=draft.currency,
                description=draft.description,
                payment_metadata=draft.metadata,
                webhook_url=draft.webhook_url,
                idempotency_key=key,
            )
            .on_conflict_do_nothing(constraint="uq_payments_idempotency_key")
            .returning(PaymentRecord)
        )
        return to_payment(row) if row is not None else None

    async def get(self, payment_id: UUID, *, for_update: bool = False) -> Payment | None:
        query = select(PaymentRecord).where(PaymentRecord.id == payment_id)
        if for_update:
            query = query.with_for_update()
        row = await self.session.scalar(query)
        return to_payment(row) if row is not None else None

    async def get_by_key(self, key: str) -> Payment | None:
        row = await self.session.scalar(
            select(PaymentRecord).where(PaymentRecord.idempotency_key == key)
        )
        return to_payment(row) if row is not None else None

    async def save(self, payment: Payment) -> None:
        result = await self.session.execute(
            update(PaymentRecord)
            .where(PaymentRecord.id == payment.id)
            .values(
                status=payment.status,
                processed_at=payment.processed_at,
                webhook_delivered_at=payment.webhook_delivered_at,
            )
        )
        if result.rowcount != 1:
            raise PaymentNotFound


class SqlAlchemyOutboxRepository:
    def __init__(self, session: AsyncSession):
        self.session = session

    async def add(self, event: OutboxEvent) -> None:
        self.session.add(
            OutboxRecord(
                id=event.id,
                event_type=event.event_type,
                payload=event.payload,
                created_at=event.created_at,
                published_at=event.published_at,
            )
        )

    async def next_unpublished(self) -> OutboxEvent | None:
        row = await self.session.scalar(
            select(OutboxRecord)
            .where(OutboxRecord.event_type == "payments.new", OutboxRecord.published_at.is_(None))
            .order_by(OutboxRecord.created_at, OutboxRecord.id)
            .limit(1)
            .with_for_update(skip_locked=True)
        )
        if row is None:
            return None
        return OutboxEvent(row.id, row.event_type, row.payload, row.created_at, row.published_at)

    async def mark_published(self, event_id: UUID, at: datetime) -> None:
        await self.session.execute(
            update(OutboxRecord).where(OutboxRecord.id == event_id).values(published_at=at)
        )
