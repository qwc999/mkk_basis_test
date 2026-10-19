import asyncio
from uuid import UUID

import pytest
from sqlalchemy import update

from app.models import Payment
from tests.test_payments import row_counts


async def test_identical_replay(client, headers, payment_data, session_factory):
    first = await client.post("/api/v1/payments", headers=headers, json=payment_data)
    second = await client.post("/api/v1/payments", headers=headers, json=payment_data)
    assert first.status_code == second.status_code == 202
    assert first.json() == second.json()
    assert await row_counts(session_factory) == (1, 1)


async def test_normalized_replay(client, headers, payment_data, session_factory):
    payment_data.update(amount="1500.0", metadata={"nested": {"a": 1, "b": [2, 3]}, "z": 4})
    first = await client.post("/api/v1/payments", headers=headers, json=payment_data)
    payment_data.update(amount="1500.00", metadata={"z": 4, "nested": {"b": [2, 3], "a": 1}})
    second = await client.post("/api/v1/payments", headers=headers, json=payment_data)
    assert first.status_code == second.status_code == 202
    assert first.json() == second.json()
    assert await row_counts(session_factory) == (1, 1)


async def test_omitted_and_empty_metadata_are_equivalent(
    client, headers, payment_data, session_factory
):
    del payment_data["metadata"]
    first = await client.post("/api/v1/payments", headers=headers, json=payment_data)
    payment_data["metadata"] = {}
    second = await client.post("/api/v1/payments", headers=headers, json=payment_data)
    assert first.status_code == second.status_code == 202
    assert first.json() == second.json()
    assert await row_counts(session_factory) == (1, 1)


async def test_replay_uses_stored_url_normalization(client, headers, payment_data):
    payment_data["webhook_url"] = "https://EXAMPLE.com:443"
    first = await client.post("/api/v1/payments", headers=headers, json=payment_data)
    payment_data["webhook_url"] = "https://example.com/"
    second = await client.post("/api/v1/payments", headers=headers, json=payment_data)
    assert first.status_code == second.status_code == 202
    assert first.json() == second.json()


@pytest.mark.parametrize(
    ("field", "value"),
    [
        ("amount", "1500.01"),
        ("currency", "USD"),
        ("description", "Другой заказ"),
        ("metadata", {"order_id": "order-456"}),
        ("webhook_url", "https://example.com/different-hook"),
    ],
)
async def test_conflicting_replay(client, headers, payment_data, session_factory, field, value):
    first = await client.post("/api/v1/payments", headers=headers, json=payment_data)
    payment_data[field] = value
    second = await client.post("/api/v1/payments", headers=headers, json=payment_data)
    assert first.status_code == 202
    assert second.status_code == 409
    assert second.json() == {"detail": "Idempotency-Key was used with different data"}
    assert await row_counts(session_factory) == (1, 1)


async def test_metadata_boolean_and_number_are_different(client, headers, payment_data):
    payment_data["metadata"] = {"value": True}
    assert (
        await client.post("/api/v1/payments", headers=headers, json=payment_data)
    ).status_code == 202
    payment_data["metadata"] = {"value": 1}
    assert (
        await client.post("/api/v1/payments", headers=headers, json=payment_data)
    ).status_code == 409


async def test_replay_returns_current_status(client, headers, payment_data, session_factory):
    first = await client.post("/api/v1/payments", headers=headers, json=payment_data)
    assert first.status_code == 202
    async with session_factory.begin() as session:
        await session.execute(
            update(Payment)
            .where(Payment.id == UUID(first.json()["payment_id"]))
            .values(status="succeeded")
        )
    second = await client.post("/api/v1/payments", headers=headers, json=payment_data)
    assert second.status_code == 202
    assert second.json() == {**first.json(), "status": "succeeded"}
    assert await row_counts(session_factory) == (1, 1)


async def test_concurrent_identical_requests(client, headers, payment_data, session_factory):
    ready = asyncio.Event()

    async def send():
        await ready.wait()
        return await client.post("/api/v1/payments", headers=headers, json=payment_data)

    tasks = [asyncio.create_task(send()) for _ in range(8)]
    ready.set()
    responses = await asyncio.wait_for(asyncio.gather(*tasks), timeout=20)
    assert [response.status_code for response in responses] == [202] * 8
    assert all(response.json() == responses[0].json() for response in responses)
    assert await row_counts(session_factory) == (1, 1)


async def test_concurrent_conflicting_requests(client, headers, payment_data, session_factory):
    responses = await asyncio.wait_for(
        asyncio.gather(
            client.post("/api/v1/payments", headers=headers, json=payment_data),
            client.post(
                "/api/v1/payments", headers=headers, json={**payment_data, "amount": "1.00"}
            ),
        ),
        timeout=20,
    )
    assert sorted(response.status_code for response in responses) == [202, 409]
    assert await row_counts(session_factory) == (1, 1)


async def test_different_keys_create_separate_payments(
    client, headers, payment_data, session_factory
):
    first = await client.post("/api/v1/payments", headers=headers, json=payment_data)
    headers["Idempotency-Key"] = "another-key"
    second = await client.post("/api/v1/payments", headers=headers, json=payment_data)
    assert first.status_code == second.status_code == 202
    assert first.json()["payment_id"] != second.json()["payment_id"]
    assert await row_counts(session_factory) == (2, 2)
