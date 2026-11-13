from dataclasses import replace
from uuid import uuid4

import pytest

from app.application.errors import StorageError
from app.application.payments import create_payment, get_payment
from app.domain.errors import IdempotencyConflict, PaymentNotFound
from app.domain.models import json_equal


@pytest.mark.parametrize(
    ("left", "right", "equal"),
    [
        ({"a": 1, "b": [2]}, {"b": [2.0], "a": 1.0}, True),
        ({"a": [True]}, {"a": [1]}, False),
        ({"a": False}, {"a": 0.0}, False),
        ([1, 2], [2, 1], False),
        ({"a": None}, {}, False),
    ],
)
def test_domain_metadata_comparison(left, right, equal):
    assert json_equal(left, right) is equal


async def test_create_and_replay_are_atomic_and_idempotent(store, draft):
    first = await create_payment(store, draft, "key")
    replay = await create_payment(store, draft, "key")
    assert first == replay == await get_payment(store, first.id)
    assert len(store.payments) == len(store.events) == 1
    assert next(iter(store.events.values())).payload == {"payment_id": str(first.id)}
    with pytest.raises(IdempotencyConflict):
        await create_payment(store, replace(draft, metadata={"paid": 1}), "key")
    assert len(store.payments) == len(store.events) == 1 and store.active == 0


async def test_outbox_failure_does_not_commit_payment(store, draft):
    store.fail_outbox = True
    with pytest.raises(StorageError):
        await create_payment(store, draft, "key")
    assert store.payments == store.events == {} and store.commits == store.active == 0


async def test_unknown_payment_is_a_domain_error(store):
    with pytest.raises(PaymentNotFound):
        await get_payment(store, uuid4())
    assert store.active == store.commits == 0
