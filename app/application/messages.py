from dataclasses import dataclass, field
from enum import StrEnum
from typing import Literal

type ErrorCategory = Literal[
    "invalid_message", "payment_not_found", "processing_error", "webhook_error"
]


class Destination(StrEnum):
    NEW = "payments.new"
    RETRY_1S = "payments.retry.1s"
    RETRY_2S = "payments.retry.2s"
    DEAD_LETTER = "payments.dlq"


@dataclass(frozen=True)
class EventMessage:
    body: bytes
    message_id: str | None
    headers: dict[str, object] = field(default_factory=dict)
