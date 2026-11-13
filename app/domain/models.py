from dataclasses import dataclass, replace
from datetime import datetime
from decimal import Decimal
from typing import Literal, Self
from uuid import UUID

from app.domain.errors import InvalidPaymentState

type JsonValue = None | bool | int | float | str | list[JsonValue] | dict[str, JsonValue]
type FinalStatus = Literal["succeeded", "failed"]
type PaymentStatus = Literal["pending", "succeeded", "failed"]


def json_equal(left: JsonValue, right: JsonValue) -> bool:
    """JSON numeric equality, with booleans distinct from numbers as in JSONB."""
    if isinstance(left, bool) or isinstance(right, bool):
        return type(left) is type(right) and left == right
    if isinstance(left, dict) and isinstance(right, dict):
        return left.keys() == right.keys() and all(json_equal(left[k], right[k]) for k in left)
    if isinstance(left, list) and isinstance(right, list):
        return len(left) == len(right) and all(
            json_equal(a, b) for a, b in zip(left, right, strict=True)
        )
    return left == right


@dataclass(frozen=True)
class PaymentDraft:
    amount: Decimal
    currency: str
    description: str
    metadata: dict[str, JsonValue]
    webhook_url: str


@dataclass(frozen=True)
class Payment:
    id: UUID
    amount: Decimal
    currency: str
    description: str
    metadata: dict[str, JsonValue]
    webhook_url: str
    idempotency_key: str
    created_at: datetime
    status: PaymentStatus = "pending"
    processed_at: datetime | None = None
    webhook_delivered_at: datetime | None = None

    def matches(self, draft: PaymentDraft) -> bool:
        return (
            self.amount == draft.amount
            and self.currency == draft.currency
            and self.description == draft.description
            and self.webhook_url == draft.webhook_url
            and json_equal(self.metadata, draft.metadata)
        )

    def complete(self, result: FinalStatus, at: datetime) -> Self:
        if self.status != "pending":
            return self
        if result not in {"succeeded", "failed"}:
            raise InvalidPaymentState
        return replace(self, status=result, processed_at=at)

    def delivered(self, at: datetime) -> Self:
        self.require_final_result()
        return self if self.webhook_delivered_at else replace(self, webhook_delivered_at=at)

    def require_final_result(self) -> None:
        if self.status not in {"succeeded", "failed"} or self.processed_at is None:
            raise InvalidPaymentState


@dataclass(frozen=True)
class OutboxEvent:
    id: UUID
    event_type: str
    payload: dict[str, JsonValue]
    created_at: datetime
    published_at: datetime | None = None
