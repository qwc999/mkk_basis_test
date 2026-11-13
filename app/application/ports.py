from collections.abc import Callable, Mapping
from datetime import datetime
from types import TracebackType
from typing import Protocol, Self
from uuid import UUID

from app.application.messages import Destination, EventMessage
from app.domain.models import FinalStatus, OutboxEvent, Payment, PaymentDraft


class PaymentRepository(Protocol):
    async def add_if_absent(self, draft: PaymentDraft, key: str) -> Payment | None: ...
    async def get(self, payment_id: UUID, *, for_update: bool = False) -> Payment | None: ...
    async def get_by_key(self, key: str) -> Payment | None: ...
    async def save(self, payment: Payment) -> None: ...


class OutboxRepository(Protocol):
    async def add(self, event: OutboxEvent) -> None: ...
    async def next_unpublished(self) -> OutboxEvent | None: ...
    async def mark_published(self, event_id: UUID, at: datetime) -> None: ...


class UnitOfWork(Protocol):
    payments: PaymentRepository
    outbox: OutboxRepository

    async def __aenter__(self) -> Self: ...
    async def __aexit__(
        self,
        exc_type: type[BaseException] | None,
        exc: BaseException | None,
        traceback: TracebackType | None,
    ) -> None: ...
    async def commit(self) -> None: ...


type UnitOfWorkFactory = Callable[[], UnitOfWork]


class PaymentGateway(Protocol):
    async def charge(self, payment: Payment) -> FinalStatus: ...


class WebhookSender(Protocol):
    async def send(self, payment: Payment) -> None: ...


class EventPublisher(Protocol):
    """Return only after the destination accepted the event, otherwise raise PublicationError."""

    async def publish(self, message: EventMessage, destination: Destination) -> None: ...


class MessageDelivery(Protocol):
    body: bytes
    headers: Mapping[str, object]
    message_id: str | None

    async def ack(self) -> None: ...
