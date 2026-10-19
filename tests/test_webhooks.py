import json
from datetime import UTC, datetime
from unittest.mock import AsyncMock
from uuid import UUID

import httpx
import pytest

from app.models import Payment
from app.processing import process_payment
from app.webhooks import WebhookDeliveryError, create_http_client, deliver_webhook


@pytest.mark.parametrize("status", ["succeeded", "failed"])
@pytest.mark.parametrize("http_status", [200, 204])
async def test_webhook_contract_and_duplicate_delivery(
    client, headers, payment_data, session_factory, engine, status, http_status
):
    response = await client.post("/api/v1/payments", headers=headers, json=payment_data)
    payment_id = UUID(response.json()["payment_id"])
    await process_payment(session_factory, payment_id, AsyncMock(return_value=status))
    async with session_factory() as session:
        payment = await session.get(Payment, payment_id)
        processed_at = payment.processed_at
        assert payment.webhook_delivered_at is None
    requests = []

    def receive(request):
        assert engine.pool.checkedout() == 0
        requests.append(request)
        assert request.method == "POST"
        assert str(request.url) == payment_data["webhook_url"]
        assert request.headers["Content-Type"] == "application/json"
        assert request.headers["Idempotency-Key"] == str(payment_id)
        assert all(value == 5 for value in request.extensions["timeout"].values())
        assert json.loads(request.content) == {
            "payment_id": str(payment_id),
            "status": status,
            "amount": "1500.00",
            "currency": "RUB",
            "processed_at": processed_at.isoformat(),
        }
        return httpx.Response(http_status)

    async with create_http_client() as http:
        assert http.timeout == httpx.Timeout(5) and http.follow_redirects is False
    async with httpx.AsyncClient(transport=httpx.MockTransport(receive)) as http:
        before = datetime.now(UTC)
        await deliver_webhook(session_factory, payment_id, http)
        async with session_factory() as session:
            payment = await session.get(Payment, payment_id)
            delivered_at = payment.webhook_delivered_at
            assert before <= delivered_at <= datetime.now(UTC)
            assert delivered_at.utcoffset().total_seconds() == 0
        await deliver_webhook(session_factory, payment_id, http)
    assert len(requests) == 1
    async with session_factory() as session:
        payment = await session.get(Payment, payment_id)
        assert payment.webhook_delivered_at == delivered_at
        assert (payment.status, payment.processed_at) == (status, processed_at)


@pytest.mark.parametrize("failure", [302, 400, 500, httpx.ConnectError, httpx.ReadTimeout])
async def test_failed_webhook_has_no_delivery_mark(
    client, headers, payment_data, session_factory, failure
):
    response = await client.post("/api/v1/payments", headers=headers, json=payment_data)
    payment_id = UUID(response.json()["payment_id"])
    await process_payment(session_factory, payment_id, AsyncMock(return_value="succeeded"))
    requests = []

    def receive(request):
        requests.append(request)
        if isinstance(failure, type):
            raise failure("sensitive URL and response must not be logged", request=request)
        return httpx.Response(failure, headers={"Location": "https://redirect.example/"})

    expected = failure if isinstance(failure, type) else WebhookDeliveryError
    # Even a client configured to follow redirects must not follow a webhook redirect.
    async with httpx.AsyncClient(
        transport=httpx.MockTransport(receive), follow_redirects=True
    ) as http:
        with pytest.raises(expected):
            await deliver_webhook(session_factory, payment_id, http)
    assert len(requests) == 1
    async with session_factory() as session:
        payment = await session.get(Payment, payment_id)
        assert payment.webhook_delivered_at is None
        assert payment.status == "succeeded" and payment.processed_at is not None
