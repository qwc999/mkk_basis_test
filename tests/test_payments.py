import json
from datetime import UTC, datetime, timedelta
from decimal import Decimal
from uuid import UUID, uuid4

import pytest
from sqlalchemy import func, select, text

from app.models import Outbox, Payment


async def row_counts(session_factory):
    async with session_factory() as session:
        return (
            await session.scalar(select(func.count()).select_from(Payment)),
            await session.scalar(select(func.count()).select_from(Outbox)),
        )


async def test_create_and_get_payment(client, headers, payment_data, session_factory):
    before = datetime.now(UTC)
    response = await client.post("/api/v1/payments", headers=headers, json=payment_data)
    after = datetime.now(UTC)
    assert response.status_code == 202, response.text
    accepted = response.json()
    assert set(accepted) == {"payment_id", "status", "created_at"}
    payment_id = UUID(accepted["payment_id"])
    assert accepted["status"] == "pending"
    created_at = datetime.fromisoformat(accepted["created_at"])
    assert created_at.utcoffset() == timedelta(0)
    assert before <= created_at <= after

    response = await client.get(f"/api/v1/payments/{payment_id}", headers=headers)
    assert response.status_code == 200
    detail = response.json()
    assert detail == {
        **payment_data,
        **accepted,
        "processed_at": None,
    }
    async with session_factory() as session:
        payment = await session.get(Payment, payment_id)
        assert payment.amount == Decimal("1500.00")
        assert isinstance(payment.amount, Decimal)
        assert payment.created_at == created_at
        assert payment.status == "pending"
        assert payment.processed_at is None
        assert payment.idempotency_key == headers["Idempotency-Key"]
        event = (await session.scalars(select(Outbox))).one()
        assert event.event_type == "payments.new"
        assert event.payload == {"payment_id": str(payment_id)}
        assert event.published_at is None
        assert before <= event.created_at <= after
        assert event.created_at.utcoffset() == timedelta(0)
    assert await row_counts(session_factory) == (1, 1)


@pytest.mark.parametrize("currency", ["RUB", "USD", "EUR"])
@pytest.mark.parametrize("amount", ["0.01", "9999999999999999.99", "1500.0000"])
async def test_valid_amounts_and_currencies(client, headers, payment_data, currency, amount):
    payment_data.update(amount=amount, currency=currency)
    response = await client.post("/api/v1/payments", headers=headers, json=payment_data)
    assert response.status_code == 202, response.text
    detail = await client.get(f"/api/v1/payments/{response.json()['payment_id']}", headers=headers)
    assert detail.json()["amount"] == format(Decimal(amount), ".2f")


async def test_default_metadata(client, headers, payment_data):
    del payment_data["metadata"]
    response = await client.post("/api/v1/payments", headers=headers, json=payment_data)
    assert response.status_code == 202
    detail = await client.get(f"/api/v1/payments/{response.json()['payment_id']}", headers=headers)
    assert detail.json()["metadata"] == {}


@pytest.mark.parametrize(
    "metadata",
    [{"nested": [1.5, True, None, {"value": "text"}]}, {"escaped": r"\u0000"}],
)
async def test_valid_metadata_round_trip(client, headers, payment_data, metadata):
    payment_data["metadata"] = metadata
    response = await client.post("/api/v1/payments", headers=headers, json=payment_data)
    assert response.status_code == 202, response.text
    detail = await client.get(f"/api/v1/payments/{response.json()['payment_id']}", headers=headers)
    assert detail.json()["metadata"] == metadata


@pytest.mark.parametrize("number", ["NaN", "Infinity", "1e999"])
async def test_nonfinite_metadata_is_rejected(client, headers, payment_data, number):
    raw_body = json.dumps(payment_data).replace(
        json.dumps(payment_data["metadata"]), '{"invalid":' + number + "}"
    )
    response = await client.post(
        "/api/v1/payments",
        headers={**headers, "Content-Type": "application/json"},
        content=raw_body,
    )
    assert response.status_code == 422, response.text


@pytest.mark.parametrize("number", ["0.01", "1500.0000", "9999999999999999.99"])
async def test_json_numeric_amount_keeps_decimal_precision(client, headers, payment_data, number):
    raw_body = json.dumps(payment_data).replace('"1500.00"', number, 1)
    response = await client.post(
        "/api/v1/payments",
        headers={**headers, "Content-Type": "application/json"},
        content=raw_body,
    )
    assert response.status_code == 202, response.text
    detail = await client.get(f"/api/v1/payments/{response.json()['payment_id']}", headers=headers)
    assert detail.json()["amount"] == format(Decimal(number), ".2f")


@pytest.mark.parametrize(
    "number",
    [
        "1.00000000000000001",
        "9999999999999999.999",
        "0.010000000000000001",
        "1e999",
        "NaN",
        "Infinity",
    ],
)
async def test_json_numeric_amount_is_not_rounded(client, headers, payment_data, number):
    raw_body = json.dumps(payment_data).replace('"1500.00"', number, 1)
    response = await client.post(
        "/api/v1/payments",
        headers={**headers, "Content-Type": "application/json"},
        content=raw_body,
    )
    assert response.status_code == 422, response.text


@pytest.mark.parametrize("body", ["{broken", "[]", "null"])
async def test_invalid_json_body(client, headers, body):
    response = await client.post(
        "/api/v1/payments",
        headers={**headers, "Content-Type": "application/json"},
        content=body,
    )
    assert response.status_code == 422


@pytest.mark.parametrize("api_key", [None, "", "wrong-key"])
@pytest.mark.parametrize("method", ["post", "get"])
async def test_authentication(client, headers, payment_data, session_factory, api_key, method):
    if api_key is None:
        headers.pop("X-API-Key")
    else:
        headers["X-API-Key"] = api_key
    if method == "post":
        response = await client.post("/api/v1/payments", headers=headers, json=payment_data)
    else:
        response = await client.get(f"/api/v1/payments/{uuid4()}", headers=headers)
    assert response.status_code == 401
    assert response.json() == {"detail": "Invalid or missing API key"}
    assert await row_counts(session_factory) == (0, 0)


@pytest.mark.parametrize("path", ["/docs", "/redoc", "/openapi.json"])
async def test_documentation_is_disabled(client, path):
    assert (await client.get(path)).status_code == 404


@pytest.mark.parametrize("key", [None, "", " ", "x" * 256, "bad\x00key"])
async def test_invalid_idempotency_key(client, headers, payment_data, session_factory, key):
    if key is None:
        headers.pop("Idempotency-Key")
    else:
        headers["Idempotency-Key"] = key
    response = await client.post("/api/v1/payments", headers=headers, json=payment_data)
    assert response.status_code == 422
    assert await row_counts(session_factory) == (0, 0)


async def test_maximum_idempotency_key_length(client, headers, payment_data):
    headers["Idempotency-Key"] = "x" * 255
    assert (
        await client.post("/api/v1/payments", headers=headers, json=payment_data)
    ).status_code == 202


@pytest.mark.parametrize(
    ("field", "value"),
    [
        ("amount", "0"),
        ("amount", "-1"),
        ("amount", "0.001"),
        ("amount", "1500.001"),
        ("amount", "10000000000000000.00"),
        ("amount", "NaN"),
        ("amount", "Infinity"),
        ("amount", "-Infinity"),
        ("amount", "invalid"),
        ("amount", True),
        ("amount", None),
        ("currency", "rub"),
        ("currency", "GBP"),
        ("currency", None),
        ("description", ""),
        ("description", " \t\n"),
        ("description", "a" * 1001),
        ("description", 123),
        ("description", "bad\x00text"),
        ("webhook_url", "not-a-url"),
        ("webhook_url", "ftp://example.com/hook"),
        ("webhook_url", "https://"),
        ("webhook_url", None),
        ("metadata", []),
        ("metadata", "text"),
        ("metadata", None),
        ("metadata", {"null": "\x00"}),
        ("unknown", "unexpected"),
    ],
)
async def test_invalid_payload(client, headers, payment_data, session_factory, field, value):
    payment_data[field] = value
    response = await client.post("/api/v1/payments", headers=headers, json=payment_data)
    assert response.status_code == 422, response.text
    assert await row_counts(session_factory) == (0, 0)


@pytest.mark.parametrize("field", ["amount", "currency", "description", "webhook_url"])
async def test_required_fields(client, headers, payment_data, field):
    del payment_data[field]
    assert (
        await client.post("/api/v1/payments", headers=headers, json=payment_data)
    ).status_code == 422


async def test_unknown_payment(client, headers):
    response = await client.get(f"/api/v1/payments/{uuid4()}", headers=headers)
    assert response.status_code == 404
    assert response.json() == {"detail": "Payment not found"}


async def test_invalid_payment_id(client, headers):
    response = await client.get("/api/v1/payments/not-a-uuid", headers=headers)
    assert response.status_code == 422


async def test_outbox_failure_rolls_back_payment_and_event(
    client, headers, payment_data, engine, session_factory
):
    # A real PostgreSQL constraint error occurs after the payment INSERT,
    # when the Outbox row is flushed at transaction commit.
    async with engine.begin() as connection:
        await connection.execute(
            text(
                "ALTER TABLE outbox ADD CONSTRAINT reject_event "
                "CHECK (event_type <> 'payments.new')"
            )
        )
    try:
        response = await client.post("/api/v1/payments", headers=headers, json=payment_data)
        assert response.status_code == 500
        assert await row_counts(session_factory) == (0, 0)
    finally:
        async with engine.begin() as connection:
            await connection.execute(text("ALTER TABLE outbox DROP CONSTRAINT reject_event"))
    retry = await client.post("/api/v1/payments", headers=headers, json=payment_data)
    assert retry.status_code == 202
    assert await row_counts(session_factory) == (1, 1)
