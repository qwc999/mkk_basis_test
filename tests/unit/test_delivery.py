import json
from types import SimpleNamespace
from unittest.mock import AsyncMock

import pytest

from app.application.consumer import consume_payment
from app.application.errors import PublicationError, StorageError, WebhookDeliveryError
from app.application.messages import Destination
from app.application.outbox import publish_one
from app.application.payments import create_payment
from app.application.ports import EventPublisher, PaymentGateway, WebhookSender
from app.application.processing import process_payment
from app.application.webhooks import deliver_webhook
from app.domain.errors import InvalidPaymentState


async def test_pending_payment_cannot_be_delivered(store, draft):
    payment = await create_payment(store, draft, "key")
    sender = AsyncMock(spec=WebhookSender)
    with pytest.raises(InvalidPaymentState):
        await deliver_webhook(store, payment.id, sender)
    sender.send.assert_not_awaited()
    assert store.payments[payment.id] == payment and store.active == 0


async def test_webhook_runs_outside_uow_and_duplicate_delivery_is_skipped(store, draft):
    payment = await create_payment(store, draft, "key")
    gateway = AsyncMock(spec=PaymentGateway)
    gateway.charge.return_value = "failed"
    await process_payment(store, payment.id, gateway)
    original = store.payments[payment.id]

    async def send(result):
        assert store.active == 0 and result == original

    sender = AsyncMock(spec=WebhookSender)
    sender.send.side_effect = send
    await deliver_webhook(store, payment.id, sender)
    delivered = store.payments[payment.id]
    assert delivered.webhook_delivered_at is not None
    assert delivered.status == original.status and delivered.processed_at == original.processed_at
    await deliver_webhook(store, payment.id, sender)
    sender.send.assert_awaited_once()


async def test_webhook_retry_only_repeats_unfinished_delivery(store, draft):
    payment = await create_payment(store, draft, "key")
    gateway = AsyncMock(spec=PaymentGateway)
    gateway.charge.return_value = "succeeded"
    sender = AsyncMock(spec=WebhookSender)
    sender.send.side_effect = [WebhookDeliveryError(), None]
    publisher = AsyncMock(spec=EventPublisher)
    body = json.dumps({"payment_id": str(payment.id)}).encode()
    first = SimpleNamespace(body=body, message_id="event", headers={}, ack=AsyncMock())
    await consume_payment(first, store, publisher, sender, gateway)
    original = store.payments[payment.id]
    assert original.webhook_delivered_at is None
    event, destination = publisher.publish.await_args.args
    assert destination == Destination.RETRY_1S and event.headers == {"x-attempt": 2}
    assert event.body == body and event.message_id == first.message_id
    first.ack.assert_awaited_once()

    async def ack_after_commit():
        assert store.payments[payment.id].webhook_delivered_at is not None

    second = SimpleNamespace(
        body=event.body,
        message_id=event.message_id,
        headers=event.headers,
        ack=AsyncMock(side_effect=ack_after_commit),
    )
    await consume_payment(second, store, publisher, sender, gateway)
    gateway.charge.assert_awaited_once()
    assert sender.send.await_count == 2
    publisher.publish.assert_awaited_once()
    second.ack.assert_awaited_once()
    assert store.payments[payment.id].processed_at == original.processed_at


async def test_delivery_commit_and_transfer_failure_leave_message_unacknowledged(store, draft):
    payment = await create_payment(store, draft, "key")
    gateway = AsyncMock(spec=PaymentGateway)
    gateway.charge.return_value = "succeeded"
    await process_payment(store, payment.id, gateway)
    sender = AsyncMock(spec=WebhookSender)
    sender.send.side_effect = lambda payment: setattr(store, "fail_commit", True)
    publisher = AsyncMock(spec=EventPublisher)
    publisher.publish.side_effect = PublicationError
    message = SimpleNamespace(
        body=json.dumps({"payment_id": str(payment.id)}).encode(),
        message_id="event",
        headers={},
        ack=AsyncMock(),
    )
    with pytest.raises(PublicationError):
        await consume_payment(message, store, publisher, sender, gateway)
    assert store.payments[payment.id].webhook_delivered_at is None
    sender.send.assert_awaited_once()
    message.ack.assert_not_awaited()


@pytest.mark.parametrize("failure", [PublicationError, StorageError])
async def test_outbox_publication_and_commit_failure_keep_event_pending(store, draft, failure):
    await create_payment(store, draft, "key")
    publisher = AsyncMock(spec=EventPublisher)
    if failure is PublicationError:
        publisher.publish.side_effect = failure
    else:
        store.fail_commit = True
    with pytest.raises(failure):
        await publish_one(store, publisher)
    assert next(iter(store.events.values())).published_at is None
    assert store.active == 0
