import time
from unittest.mock import AsyncMock
from uuid import UUID, uuid4

import httpx
import pytest
from aio_pika import DeliveryMode
from aio_pika.exceptions import PublishError

from app.messaging import DLQ_QUEUE, DLX_EXCHANGE, RETRY_EXCHANGE, RETRY_QUEUES
from app.outbox import PublicationNotConfirmed
from app.retries import retry_or_dead_letter
from app.worker import consume_payment
from tests.test_worker import queue_empty, send, wait_until


async def test_both_retry_stages_return_through_queue_ttl(rabbit):
    broker, queue = rabbit
    body = b'{"payment_id": "' + str(uuid4()).encode() + b'"}'
    message_id = str(uuid4())
    await send(broker, body, message_id)
    for attempt, delay in ((1, 1), (2, 2)):
        message = await queue.get(timeout=3)
        assert message.headers.get("x-attempt", 1) == attempt
        assert message.body == body and message.message_id == message_id
        started = time.monotonic()
        await retry_or_dead_letter(message, broker, attempt=attempt, reason="webhook_error")
        assert message.processed
        assert await queue_empty(queue)
        retry_queue = await broker.declare_queue(RETRY_QUEUES[attempt])
        await retry_queue.declare()
        assert retry_queue.declaration_result.message_count == 1

        async def returned():
            return not await queue_empty(queue)

        await wait_until(returned)
        assert time.monotonic() - started >= delay * 0.9
    returned = await queue.get(timeout=3)
    assert returned.headers["x-attempt"] == 3
    assert returned.body == body and returned.message_id == message_id
    assert returned.delivery_mode == DeliveryMode.PERSISTENT
    await returned.ack()


async def test_third_failure_goes_to_dlq_without_another_retry(
    client, headers, payment_data, session_factory, rabbit
):
    broker, queue = rabbit
    response = await client.post("/api/v1/payments", headers=headers, json=payment_data)
    body = {"payment_id": str(UUID(response.json()["payment_id"]))}
    await send(broker, body, event_headers={"x-attempt": 3})
    message = await queue.get(timeout=3)
    requests = []

    def unavailable(request):
        requests.append(request)
        return httpx.Response(503)

    async with httpx.AsyncClient(transport=httpx.MockTransport(unavailable)) as http:
        await consume_payment(
            message, session_factory, broker, http, AsyncMock(return_value="succeeded")
        )
    assert len(requests) == 1
    dlq = await broker.declare_queue(DLQ_QUEUE)
    dead = await dlq.get(timeout=3)
    assert dead.body == message.body and dead.message_id == message.message_id
    assert dead.headers == {"x-attempt": 3, "x-error-category": "webhook_error"}
    assert dead.delivery_mode == DeliveryMode.PERSISTENT
    await dead.ack()
    assert await queue_empty(queue)
    for schema in RETRY_QUEUES.values():
        assert await queue_empty(await broker.declare_queue(schema))


@pytest.mark.parametrize("attempt", [1, 3])
@pytest.mark.parametrize("failure", ["unroutable", "no_confirm", "transport"])
async def test_failed_transfer_does_not_ack_original(rabbit, attempt, failure, monkeypatch):
    broker, queue = rabbit
    await send(broker, {"payment_id": str(uuid4())})
    message = await queue.get(timeout=3)
    schema = RETRY_QUEUES[1] if attempt == 1 else DLQ_QUEUE
    exchange_schema = RETRY_EXCHANGE if attempt == 1 else DLX_EXCHANGE
    target = await broker.declare_queue(schema)
    exchange = await broker.declare_exchange(exchange_schema)
    if failure == "unroutable":
        await target.unbind(exchange, routing_key=schema.routing_key)
        expected = PublishError
    elif failure == "no_confirm":
        monkeypatch.setattr(broker, "publish", AsyncMock(return_value=None))
        expected = PublicationNotConfirmed
    else:
        monkeypatch.setattr(broker, "publish", AsyncMock(side_effect=ConnectionError))
        expected = ConnectionError
    try:
        with pytest.raises(expected):
            await retry_or_dead_letter(message, broker, attempt=attempt, reason="webhook_error")
        assert not message.processed
    finally:
        if failure == "unroutable":
            await target.bind(exchange, routing_key=schema.routing_key)
        await message.reject(requeue=False)
