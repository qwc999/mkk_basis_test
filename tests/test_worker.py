import asyncio
import json
from contextlib import asynccontextmanager
from unittest.mock import AsyncMock
from urllib.parse import quote, urlsplit
from uuid import UUID, uuid4

import httpx
import pytest
from faststream.rabbit import Channel, RabbitBroker
from sqlalchemy import select, text

from app.messaging import PAYMENTS_EXCHANGE, PAYMENTS_QUEUE, create_broker, declare_topology
from app.models import Outbox, Payment
from app.worker import consume_payment, run_worker


async def wait_until(check):
    async with asyncio.timeout(8):
        while not await check():  # noqa: ASYNC110 -- poll state in PostgreSQL/RabbitMQ
            await asyncio.sleep(0.02)


@asynccontextmanager
async def worker(settings, simulator):
    stop = asyncio.Event()
    task = asyncio.create_task(run_worker(settings, stop, simulator))
    try:
        yield task, stop
    finally:
        stop.set()
        await asyncio.wait_for(task, 15)


async def payment_state(sessions, payment_id):
    async with sessions() as session:
        payment = await session.get(Payment, payment_id)
        return payment.status, payment.processed_at


async def final(sessions, payment_id):
    return (await payment_state(sessions, payment_id))[0] != "pending"


async def queue_empty(queue):
    await queue.declare()
    return queue.declaration_result.message_count == 0


async def send(broker, body, message_id=None):
    await broker.publish(
        body,
        exchange=PAYMENTS_EXCHANGE,
        routing_key=PAYMENTS_QUEUE.routing_key,
        persist=True,
        mandatory=True,
        timeout=2,
        message_id=message_id or str(uuid4()),
    )


@pytest.mark.parametrize("result", ["succeeded", "failed"])
async def test_worker_processes_outbox_and_ignores_duplicate_message_ids(
    client, headers, payment_data, session_factory, worker_settings, rabbit, result
):
    broker, queue = rabbit
    response = await client.post("/api/v1/payments", headers=headers, json=payment_data)
    payment_id = UUID(response.json()["payment_id"])
    simulator = AsyncMock(return_value=result)
    async with worker(worker_settings, simulator):
        await wait_until(lambda: final(session_factory, payment_id))
        original = await payment_state(session_factory, payment_id)
        assert original[0] == result and original[1] is not None
        # Deliberately use a different message_id: deduplication is by payment_id.
        await send(broker, {"payment_id": str(payment_id)}, str(uuid4()))
        await wait_until(lambda: queue_empty(queue))
    simulator.assert_awaited_once()
    assert await payment_state(session_factory, payment_id) == original
    async with session_factory() as session:
        event = (await session.scalars(select(Outbox))).one()
        assert event.published_at is not None
    assert await queue.get(fail=False) is None


async def test_technical_failure_requeues_and_is_retried(
    client, headers, payment_data, session_factory, worker_settings, rabbit, monkeypatch
):
    response = await client.post("/api/v1/payments", headers=headers, json=payment_data)
    payment_id = UUID(response.json()["payment_id"])
    simulator = AsyncMock(side_effect=[RuntimeError("technical failure"), "succeeded"])
    deliveries = []
    original = consume_payment

    async def observe(message, sessions, sim):
        deliveries.append(message.raw_message.redelivered)
        await original(message, sessions, sim)

    monkeypatch.setattr("app.worker.consume_payment", observe)
    monkeypatch.setattr("app.worker.TECHNICAL_ERROR_PAUSE", 0.05)
    async with worker(worker_settings, simulator):
        await wait_until(lambda: final(session_factory, payment_id))
    assert simulator.await_count == 2
    assert deliveries == [False, True]


@pytest.mark.parametrize(
    "body",
    [b"{broken", b"{}", b'{"payment_id":"bad"}', json.dumps({"payment_id": str(uuid4())}).encode()],
)
async def test_invalid_or_unknown_payment_is_rejected_without_requeue(
    worker_settings, session_factory, rabbit, body
):
    broker, queue = rabbit
    simulator = AsyncMock()
    # Directly retrieve the real RabbitMQ message to inspect redelivery after reject.
    await send(broker, body)
    message = await queue.get(timeout=3)
    await consume_payment(message, session_factory, simulator)
    assert message.processed
    assert await queue.get(fail=False) is None
    simulator.assert_not_awaited()


async def test_database_commit_failure_does_not_ack(
    client, headers, payment_data, session_factory, engine, rabbit, monkeypatch
):
    response = await client.post("/api/v1/payments", headers=headers, json=payment_data)
    payment_id = UUID(response.json()["payment_id"])
    broker, queue = rabbit
    await send(broker, {"payment_id": str(payment_id)})
    message = await queue.get(timeout=3)
    monkeypatch.setattr("app.worker.TECHNICAL_ERROR_PAUSE", 0.01)
    async with engine.begin() as connection:
        await connection.execute(
            text("ALTER TABLE payments ADD CONSTRAINT pending_only CHECK (status = 'pending')")
        )
    try:
        await consume_payment(message, session_factory, AsyncMock(return_value="succeeded"))
        assert await payment_state(session_factory, payment_id) == ("pending", None)
        redelivered = await queue.get(timeout=3)
        assert redelivered.redelivered
    finally:
        async with engine.begin() as connection:
            await connection.execute(text("ALTER TABLE payments DROP CONSTRAINT pending_only"))
    await consume_payment(redelivered, session_factory, AsyncMock(return_value="failed"))
    assert (await payment_state(session_factory, payment_id))[0] == "failed"


async def test_lost_ack_after_commit_is_safe_on_redelivery(
    client, headers, payment_data, session_factory, worker_settings, rabbit
):
    response = await client.post("/api/v1/payments", headers=headers, json=payment_data)
    payment_id = UUID(response.json()["payment_id"])
    broker, queue = rabbit
    await send(broker, {"payment_id": str(payment_id)})
    message = await queue.get(timeout=3)

    class LostAck:
        body = message.body

        async def ack(self):
            raise ConnectionError("connection lost before ACK")

    with pytest.raises(ConnectionError):
        await consume_payment(LostAck(), session_factory, AsyncMock(return_value="succeeded"))
    original = await payment_state(session_factory, payment_id)
    await broker.stop()  # Closing a connection returns its unacknowledged delivery.
    replacement = create_broker(worker_settings)
    try:
        await replacement.connect()
        restored_queue = await declare_topology(replacement)
        redelivered = await restored_queue.get(timeout=3)
        assert redelivered.redelivered
        simulator = AsyncMock()
        await consume_payment(redelivered, session_factory, simulator)
        simulator.assert_not_awaited()
        assert await payment_state(session_factory, payment_id) == original
    finally:
        await replacement.stop()


async def test_outbox_task_failure_stops_worker(worker_settings, engine, rabbit, monkeypatch):
    async def broken_sender(*args):
        raise RuntimeError("sender failed")

    monkeypatch.setattr("app.worker.publish_outbox", broken_sender)
    with pytest.raises(RuntimeError, match="sender failed"):
        await asyncio.wait_for(run_worker(worker_settings, asyncio.Event()), 8)
    _, queue = rabbit
    await queue.declare()
    assert queue.declaration_result.consumer_count == 0


async def test_idle_worker_stops_cleanly(worker_settings, engine, rabbit):
    _, queue = rabbit

    async def consuming():
        await queue.declare()
        return queue.declaration_result.consumer_count == 1

    async with worker(worker_settings, AsyncMock()) as (task, stop):
        await wait_until(consuming)
        stop.set()
        await asyncio.wait_for(asyncio.shield(task), 3)
    await queue.declare()
    assert queue.declaration_result.consumer_count == 0


async def test_shutdown_returns_inflight_message_and_rolls_back(
    client, headers, payment_data, session_factory, worker_settings, rabbit, monkeypatch
):
    broker, queue = rabbit
    entered = asyncio.Event()

    async def blocked_simulator():
        entered.set()
        await asyncio.Event().wait()

    def quick_shutdown_broker(settings):
        return RabbitBroker(
            settings.rabbitmq_url.get_secret_value(),
            default_channel=Channel(
                prefetch_count=1, publisher_confirms=True, on_return_raises=True
            ),
            graceful_timeout=0.05,
            logger=None,
        )

    monkeypatch.setattr("app.worker.create_broker", quick_shutdown_broker)
    response = await client.post("/api/v1/payments", headers=headers, json=payment_data)
    payment_id = UUID(response.json()["payment_id"])
    async with worker(worker_settings, blocked_simulator):
        await asyncio.wait_for(entered.wait(), 5)
    assert await payment_state(session_factory, payment_id) == ("pending", None)
    redelivered = await queue.get(timeout=3)
    assert redelivered.redelivered
    await consume_payment(redelivered, session_factory, AsyncMock(return_value="succeeded"))
    assert (await payment_state(session_factory, payment_id))[0] == "succeeded"


async def test_real_subscriber_discards_invalid_json(worker_settings, engine, rabbit):
    broker, queue = rabbit
    simulator = AsyncMock()

    async def consuming():
        await queue.declare()
        return queue.declaration_result.consumer_count == 1

    async with worker(worker_settings, simulator):
        await wait_until(consuming)
        await send(broker, b"{invalid-json")
        await wait_until(lambda: queue_empty(queue))
    simulator.assert_not_awaited()
    assert await queue.get(fail=False) is None


async def test_prefetch_one_serializes_real_handler(
    client, headers, payment_data, session_factory, worker_settings, rabbit
):
    _, queue = rabbit
    payment_ids = []
    for i in range(2):
        response = await client.post(
            "/api/v1/payments",
            headers={**headers, "Idempotency-Key": f"serial-{i}"},
            json=payment_data,
        )
        payment_ids.append(UUID(response.json()["payment_id"]))
    entered = asyncio.Event()
    release = asyncio.Event()
    calls = 0

    async def simulator():
        nonlocal calls
        calls += 1
        entered.set()
        await release.wait()
        return "succeeded"

    async def all_published():
        async with session_factory() as session:
            events = (await session.scalars(select(Outbox))).all()
            return all(event.published_at is not None for event in events)

    async with worker(worker_settings, simulator):
        try:
            await asyncio.wait_for(entered.wait(), 5)
            await wait_until(all_published)
            assert calls == 1
            await queue.declare()
            assert queue.declaration_result.message_count == 1
        finally:
            release.set()
        for payment_id in payment_ids:
            await wait_until(lambda payment_id=payment_id: final(session_factory, payment_id))
    assert calls == 2


async def test_worker_recovers_after_broker_forces_connections_closed(
    client, headers, payment_data, session_factory, worker_settings, rabbit
):
    import os

    broker_url = urlsplit(worker_settings.rabbitmq_url.get_secret_value())
    management_url = os.environ["TEST_RABBITMQ_MANAGEMENT_URL"]
    assert urlsplit(management_url).hostname == broker_url.hostname
    _, queue = rabbit

    async def consuming():
        await queue.declare()
        return queue.declaration_result.consumer_count == 1

    async with worker(worker_settings, AsyncMock(return_value="succeeded")):
        await wait_until(consuming)
        async with httpx.AsyncClient(
            base_url=management_url, auth=(broker_url.username, broker_url.password), timeout=5
        ) as management:
            connections = []

            async def connections_visible():
                nonlocal connections
                response = await management.get("/api/connections")
                response.raise_for_status()
                connections = [
                    item
                    for item in response.json()
                    if item["vhost"] == "payments_test" and item["user"] == "payments_test"
                ]
                return len(connections) >= 2

            # RabbitMQ's management statistics refresh independently of AMQP.
            await wait_until(connections_visible)
            for connection in connections:
                response = await management.delete(
                    "/api/connections/" + quote(connection["name"], safe="")
                )
                response.raise_for_status()
        response = await client.post("/api/v1/payments", headers=headers, json=payment_data)
        payment_id = UUID(response.json()["payment_id"])
        await wait_until(lambda: final(session_factory, payment_id))
