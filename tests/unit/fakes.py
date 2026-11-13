from copy import deepcopy
from dataclasses import replace
from datetime import UTC, datetime
from uuid import uuid4

from app.application.errors import StorageError
from app.domain.models import Payment


class MemoryStore:
    def __init__(self):
        self.payments = {}
        self.events = {}
        self.active = 0
        self.commits = 0
        self.fail_outbox = False
        self.fail_commit = False

    def __call__(self):
        return MemoryUnitOfWork(self)


class MemoryPayments:
    def __init__(self, uow):
        self.uow = uow

    async def add_if_absent(self, draft, key):
        if await self.get_by_key(key) is not None:
            return None
        payment = Payment(
            id=uuid4(),
            amount=draft.amount,
            currency=draft.currency,
            description=draft.description,
            metadata=deepcopy(draft.metadata),
            webhook_url=draft.webhook_url,
            idempotency_key=key,
            created_at=datetime.now(UTC),
        )
        self.uow.payment_rows[payment.id] = payment
        return payment

    async def get(self, payment_id, *, for_update=False):
        return self.uow.payment_rows.get(payment_id)

    async def get_by_key(self, key):
        return next((p for p in self.uow.payment_rows.values() if p.idempotency_key == key), None)

    async def save(self, payment):
        self.uow.payment_rows[payment.id] = payment


class MemoryOutbox:
    def __init__(self, uow):
        self.uow = uow

    async def add(self, event):
        if self.uow.store.fail_outbox:
            raise StorageError
        self.uow.event_rows[event.id] = event

    async def next_unpublished(self):
        events = sorted(self.uow.event_rows.values(), key=lambda e: (e.created_at, e.id))
        return next((e for e in events if e.published_at is None), None)

    async def mark_published(self, event_id, at):
        self.uow.event_rows[event_id] = replace(self.uow.event_rows[event_id], published_at=at)


class MemoryUnitOfWork:
    def __init__(self, store):
        self.store = store

    async def __aenter__(self):
        self.store.active += 1
        self.payment_rows = deepcopy(self.store.payments)
        self.event_rows = deepcopy(self.store.events)
        self.payments = MemoryPayments(self)
        self.outbox = MemoryOutbox(self)
        return self

    async def commit(self):
        if self.store.fail_commit:
            raise StorageError
        self.store.payments = deepcopy(self.payment_rows)
        self.store.events = deepcopy(self.event_rows)
        self.store.commits += 1

    async def __aexit__(self, *args):
        self.store.active -= 1
