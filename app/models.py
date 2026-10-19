from datetime import UTC, datetime
from decimal import Decimal
from uuid import UUID, uuid4

from sqlalchemy import (
    CheckConstraint,
    DateTime,
    Index,
    Numeric,
    String,
    Text,
    UniqueConstraint,
    func,
    text,
)
from sqlalchemy.dialects.postgresql import JSONB
from sqlalchemy.orm import Mapped, mapped_column

from app.db import Base


class Payment(Base):
    __tablename__ = "payments"
    __table_args__ = (
        UniqueConstraint("idempotency_key", name="uq_payments_idempotency_key"),
        CheckConstraint(
            "amount > 0 AND amount <= 9999999999999999.99", name="ck_payments_positive_amount"
        ),
        CheckConstraint("currency IN ('RUB', 'USD', 'EUR')", name="ck_payments_currency"),
        CheckConstraint("status IN ('pending', 'succeeded', 'failed')", name="ck_payments_status"),
        CheckConstraint("btrim(description) <> ''", name="ck_payments_description"),
        CheckConstraint("btrim(idempotency_key) <> ''", name="ck_payments_idempotency_key"),
        CheckConstraint("jsonb_typeof(metadata) = 'object'", name="ck_payments_metadata_object"),
    )

    id: Mapped[UUID] = mapped_column(primary_key=True, default=uuid4)
    amount: Mapped[Decimal] = mapped_column(Numeric(18, 2))
    currency: Mapped[str] = mapped_column(String(3))
    description: Mapped[str] = mapped_column(String(1000))
    payment_metadata: Mapped[dict] = mapped_column(
        "metadata", JSONB, default=dict, server_default="{}"
    )
    status: Mapped[str] = mapped_column(String(9), default="pending", server_default="pending")
    idempotency_key: Mapped[str] = mapped_column(String(255))
    webhook_url: Mapped[str] = mapped_column(Text)
    created_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), default=lambda: datetime.now(UTC), server_default=func.now()
    )
    processed_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True))
    webhook_delivered_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True))


class Outbox(Base):
    __tablename__ = "outbox"
    __table_args__ = (
        Index(
            "ix_outbox_unpublished_payments",
            "created_at",
            "id",
            postgresql_where=text("published_at IS NULL AND event_type = 'payments.new'"),
        ),
    )

    id: Mapped[UUID] = mapped_column(primary_key=True, default=uuid4)
    event_type: Mapped[str] = mapped_column(String(255))
    payload: Mapped[dict] = mapped_column(JSONB)
    created_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), default=lambda: datetime.now(UTC), server_default=func.now()
    )
    published_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True))
