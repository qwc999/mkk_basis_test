import asyncio
import json
from datetime import UTC, datetime
from unittest.mock import AsyncMock

import pytest
from aio_pika import DeliveryMode
from aio_pika.exceptions import ChannelInvalidStateError, PublishError
from sqlalchemy import select

from app.messaging import PAYMENTS_EXCHANGE, PAYMENTS_QUEUE
from app.models import Outbox
from app.outbox import PublicationNotConfirmed, publish_one, publish_outbox


async def create_event(client, headers, payment_data, session_factory):
    response = await client.post("/api/v1/payments", headers=headers, json=payment_data)
    assert response.status_code == 202
    async with session_factory() as session:
        return (await session.scalars(select(Outbox))).one()


async def published_at(session_factory, event_id):
    async with session_factory() as session:
        return (await session.get(Outbox, event_id)).published_at


async def test_publish_persistent_message_and_timestamp(
    client, headers, payment_data, session_factory, rabbit, monkeypatch
):
    event = await create_event(client, headers, payment_data, session_factory)
    broker, queue = rabbit
    original_publish = broker.publish
    entered = asyncio.Event()
    release = asyncio.Event()

    async def delayed_publish(*args, **kwargs):
        entered.set()
        await release.wait()
        return await original_publish(*args, **kwargs)

    monkeypatch.setattr(broker, "publish", delayed_publish)
    task = asyncio.create_task(publish_one(session_factory, broker, 5))
    before = datetime.now(UTC)
    try:
        await asyncio.wait_for(entered.wait(), 3)
        assert await published_at(session_factory, event.id) is None
        release.set()
        assert await asyncio.wait_for(task, 5)
    finally:
        task.cancel()
        await asyncio.gather(task, return_exceptions=True)
    timestamp = await published_at(session_factory, event.id)
    assert before <= timestamp <= datetime.now(UTC)
    message = await queue.get(timeout=3)
    assert json.loads(message.body) == event.payload
    assert message.message_id == str(event.id)
    assert message.delivery_mode == DeliveryMode.PERSISTENT
    await message.ack()
    assert not await publish_one(session_factory, broker, 1)


@pytest.mark.parametrize("failure", [OSError("offline"), PublicationNotConfirmed()])
async def test_publish_error_keeps_event_pending(
    client, headers, payment_data, session_factory, rabbit, monkeypatch, failure
):
    event = await create_event(client, headers, payment_data, session_factory)
    broker, _ = rabbit
    monkeypatch.setattr(broker, "publish", AsyncMock(side_effect=failure))
    with pytest.raises(type(failure)):
        await publish_one(session_factory, broker, 0.1)
    assert await published_at(session_factory, event.id) is None


async def test_publish_without_confirmation_keeps_event_pending(
    client, headers, payment_data, session_factory, rabbit, monkeypatch
):
    event = await create_event(client, headers, payment_data, session_factory)
    broker, _ = rabbit
    monkeypatch.setattr(broker, "publish", AsyncMock(return_value=None))
    with pytest.raises(PublicationNotConfirmed):
        await publish_one(session_factory, broker, 0.1)
    assert await published_at(session_factory, event.id) is None


async def test_publish_timeout_keeps_event_pending(
    client, headers, payment_data, session_factory, rabbit, monkeypatch
):
    event = await create_event(client, headers, payment_data, session_factory)
    broker, _ = rabbit

    async def stalled(*args, **kwargs):
        await asyncio.Event().wait()

    monkeypatch.setattr(broker, "publish", stalled)
    with pytest.raises(TimeoutError):
        await publish_one(session_factory, broker, 0.05)
    assert await published_at(session_factory, event.id) is None


async def test_unroutable_event_is_not_marked_published(
    client, headers, payment_data, session_factory, rabbit
):
    event = await create_event(client, headers, payment_data, session_factory)
    broker, queue = rabbit
    exchange = await broker.declare_exchange(PAYMENTS_EXCHANGE)
    await queue.unbind(exchange, routing_key=PAYMENTS_QUEUE.routing_key)
    try:
        with pytest.raises(PublishError):
            await publish_one(session_factory, broker, 1)
        assert await published_at(session_factory, event.id) is None
        assert await queue.get(fail=False) is None
    finally:
        await queue.bind(exchange, routing_key=PAYMENTS_QUEUE.routing_key)
    assert await publish_one(session_factory, broker, 1)
    message = await queue.get(timeout=3)
    await message.ack()


async def test_concurrent_senders_skip_locked_event_and_recover_after_cancel(
    client, headers, payment_data, session_factory, rabbit, monkeypatch
):
    event = await create_event(client, headers, payment_data, session_factory)
    broker, queue = rabbit
    original_publish = broker.publish
    accepted = asyncio.Event()

    async def accepted_but_no_db_commit(*args, **kwargs):
        await original_publish(*args, **kwargs)
        accepted.set()
        await asyncio.Event().wait()

    monkeypatch.setattr(broker, "publish", accepted_but_no_db_commit)
    first = asyncio.create_task(publish_one(session_factory, broker, 10))
    try:
        await asyncio.wait_for(accepted.wait(), 5)
        assert not await asyncio.wait_for(publish_one(session_factory, broker, 1), 3)
    finally:
        first.cancel()
        await asyncio.gather(first, return_exceptions=True)
    assert await published_at(session_factory, event.id) is None
    monkeypatch.setattr(broker, "publish", original_publish)
    assert await publish_one(session_factory, broker, 1)
    # At-least-once window: RabbitMQ accepted the first attempt, but the DB rolled back.
    messages = [await queue.get(timeout=3), await queue.get(timeout=3)]
    assert all(message.message_id == str(event.id) for message in messages)
    for message in messages:
        await message.ack()


async def test_concurrent_senders_publish_each_event_once(
    client, headers, payment_data, session_factory, rabbit
):
    broker, queue = rabbit
    for i in range(4):
        response = await client.post(
            "/api/v1/payments",
            headers={**headers, "Idempotency-Key": f"parallel-{i}"},
            json=payment_data,
        )
        assert response.status_code == 202
    assert all(
        await asyncio.wait_for(
            asyncio.gather(*[publish_one(session_factory, broker, 2) for _ in range(4)]), 10
        )
    )
    messages = [await queue.get(timeout=3) for _ in range(4)]
    assert len({message.message_id for message in messages}) == 4
    assert await queue.get(fail=False) is None
    for message in messages:
        await message.ack()


async def test_sender_continues_after_temporarily_unavailable_channel(
    client, headers, payment_data, session_factory, worker_settings, rabbit, monkeypatch
):
    event = await create_event(client, headers, payment_data, session_factory)
    broker, queue = rabbit
    original_publish = broker.publish
    attempts = 0

    async def interrupted_publish(*args, **kwargs):
        nonlocal attempts
        attempts += 1
        if attempts == 1:
            raise ChannelInvalidStateError("channel temporarily unavailable")
        return await original_publish(*args, **kwargs)

    monkeypatch.setattr(broker, "publish", interrupted_publish)
    sender = asyncio.create_task(publish_outbox(session_factory, broker, worker_settings))
    try:
        async with asyncio.timeout(5):
            while await published_at(session_factory, event.id) is None:
                if sender.done():
                    await sender
                await asyncio.sleep(0.02)
    finally:
        sender.cancel()
        await asyncio.gather(sender, return_exceptions=True)
    assert attempts == 2
    message = await queue.get(timeout=3)
    assert message.message_id == str(event.id)
    await message.ack()
