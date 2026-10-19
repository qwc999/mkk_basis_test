from unittest.mock import AsyncMock
from uuid import UUID

import httpx
from sqlalchemy import text

from app.models import Payment
from app.worker import consume_payment
from tests.test_worker import payment_state, queue_empty, send, wait_until


async def test_webhook_retry_preserves_result_and_skips_simulator(
    client, headers, payment_data, session_factory, rabbit, caplog
):
    response = await client.post("/api/v1/payments", headers=headers, json=payment_data)
    payment_id = UUID(response.json()["payment_id"])
    broker, queue = rabbit
    await send(broker, {"payment_id": str(payment_id)})
    simulator = AsyncMock(return_value="failed")
    requests = []

    def receive(request):
        requests.append(request)
        return httpx.Response(500 if len(requests) == 1 else 204, text="sensitive-response")

    async with httpx.AsyncClient(transport=httpx.MockTransport(receive)) as http:
        first = await queue.get(timeout=3)
        await consume_payment(first, session_factory, broker, http, simulator)
        original = await payment_state(session_factory, payment_id)
        assert original[0] == "failed" and original[1] is not None
        async with session_factory() as session:
            assert (await session.get(Payment, payment_id)).webhook_delivered_at is None

        async def returned():
            return not await queue_empty(queue)

        await wait_until(returned)
        retry = await queue.get(timeout=3)
        assert retry.headers["x-attempt"] == 2
        await consume_payment(retry, session_factory, broker, http, simulator)
        assert retry.processed
    simulator.assert_awaited_once()
    assert await payment_state(session_factory, payment_id) == original
    async with session_factory() as session:
        assert (await session.get(Payment, payment_id)).webhook_delivered_at is not None
    assert len(requests) == 2
    assert all(request.headers["Idempotency-Key"] == str(payment_id) for request in requests)
    assert "sensitive-response" not in caplog.text
    assert payment_data["webhook_url"] not in caplog.text


async def test_delivery_commit_failure_requires_retry_handoff_before_ack(
    client, headers, payment_data, session_factory, engine, rabbit, webhook_client, monkeypatch
):
    response = await client.post("/api/v1/payments", headers=headers, json=payment_data)
    payment_id = UUID(response.json()["payment_id"])
    broker, queue = rabbit
    await send(broker, {"payment_id": str(payment_id)})
    message = await queue.get(timeout=3)
    publish = broker.publish
    transfers = []

    async def guarded_publish(*args, **kwargs):
        # A successful HTTP response alone must never ACK the original delivery.
        assert not message.processed
        async with session_factory() as session:
            payment = await session.get(Payment, payment_id)
            assert payment.status == "succeeded" and payment.processed_at is not None
            assert payment.webhook_delivered_at is None
        transfers.append(kwargs)
        return await publish(*args, **kwargs)

    monkeypatch.setattr(broker, "publish", guarded_publish)
    async with engine.begin() as connection:
        await connection.execute(
            text(
                "ALTER TABLE payments ADD CONSTRAINT delivery_blocked "
                "CHECK (webhook_delivered_at IS NULL)"
            )
        )
    try:
        await consume_payment(
            message, session_factory, broker, webhook_client, AsyncMock(return_value="succeeded")
        )
        assert len(transfers) == 1
        assert transfers[0]["routing_key"] == "payments.retry.1s"
        assert transfers[0]["headers"]["x-attempt"] == 2
        # ACK is permitted only now, after confirmed transfer to the retry queue.
        assert message.processed
    finally:
        async with engine.begin() as connection:
            await connection.execute(text("ALTER TABLE payments DROP CONSTRAINT delivery_blocked"))
