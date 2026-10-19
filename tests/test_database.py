from decimal import Decimal
from uuid import uuid4

import pytest
from sqlalchemy import insert, text
from sqlalchemy.exc import DBAPIError, IntegrityError

from app.models import Payment
from tests.test_payments import row_counts


async def test_migration_schema(engine):
    async with engine.connect() as connection:
        assert await connection.scalar(text("SELECT version_num FROM alembic_version")) == "0002"
        columns = (
            await connection.execute(
                text(
                    "SELECT column_name, data_type, numeric_precision, numeric_scale "
                    "FROM information_schema.columns WHERE table_name = 'payments'"
                )
            )
        ).all()
        column_info = {row[0]: row[1:] for row in columns}
        assert column_info["amount"] == ("numeric", 18, 2)
        assert column_info["created_at"][0] == "timestamp with time zone"
        assert column_info["processed_at"][0] == "timestamp with time zone"
        assert column_info["metadata"][0] == "jsonb"


@pytest.mark.parametrize(
    ("field", "value", "constraint"),
    [
        ("amount", Decimal("0"), "ck_payments_positive_amount"),
        ("amount", Decimal("-1"), "ck_payments_positive_amount"),
        ("amount", Decimal("NaN"), "ck_payments_positive_amount"),
        ("currency", "GBP", "ck_payments_currency"),
        ("status", "unknown", "ck_payments_status"),
        ("description", "", "ck_payments_description"),
        ("idempotency_key", "", "ck_payments_idempotency_key"),
        ("payment_metadata", [], "ck_payments_metadata_object"),
    ],
)
async def test_database_constraints(session_factory, field, value, constraint):
    values = {
        "amount": Decimal("1.00"),
        "currency": "RUB",
        "description": "Test",
        "status": "pending",
        "idempotency_key": str(uuid4()),
        "webhook_url": "https://example.com/",
    }
    values[field] = value
    with pytest.raises(IntegrityError) as exc:
        async with session_factory.begin() as session:
            await session.execute(insert(Payment).values(**values))
    assert constraint in str(exc.value.orig)
    assert await row_counts(session_factory) == (0, 0)


async def test_database_unique_idempotency_key(client, headers, payment_data, session_factory):
    response = await client.post("/api/v1/payments", headers=headers, json=payment_data)
    assert response.status_code == 202
    with pytest.raises(IntegrityError) as exc:
        async with session_factory.begin() as session:
            await session.execute(
                insert(Payment).values(
                    amount=Decimal("1.00"),
                    currency="RUB",
                    description="Duplicate",
                    idempotency_key=headers["Idempotency-Key"],
                    webhook_url="https://example.com/",
                )
            )
    assert "uq_payments_idempotency_key" in str(exc.value.orig)
    assert await row_counts(session_factory) == (1, 1)


async def test_database_numeric_overflow(session_factory):
    with pytest.raises(DBAPIError):
        async with session_factory.begin() as session:
            await session.execute(
                insert(Payment).values(
                    amount=Decimal("10000000000000000.00"),
                    currency="RUB",
                    description="Too large",
                    idempotency_key="overflow",
                    webhook_url="https://example.com/",
                )
            )
    assert await row_counts(session_factory) == (0, 0)
