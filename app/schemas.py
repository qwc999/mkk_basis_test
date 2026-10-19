import json
from datetime import datetime
from decimal import Decimal
from typing import Annotated, Literal
from uuid import UUID

from pydantic import (
    BaseModel,
    ConfigDict,
    Field,
    HttpUrl,
    JsonValue,
    field_serializer,
    field_validator,
)

Currency = Literal["RUB", "USD", "EUR"]
PaymentStatus = Literal["pending", "succeeded", "failed"]
Amount = Annotated[Decimal, Field(gt=0, max_digits=18, decimal_places=2, allow_inf_nan=False)]
IdempotencyKey = Annotated[str, Field(min_length=1, max_length=255, pattern=r"\S")]


def contains_null_character(value: JsonValue) -> bool:
    if isinstance(value, str):
        return "\x00" in value
    if isinstance(value, dict):
        return any("\x00" in key or contains_null_character(item) for key, item in value.items())
    if isinstance(value, list):
        return any(contains_null_character(item) for item in value)
    return False


class PaymentCreate(BaseModel):
    model_config = ConfigDict(extra="forbid")

    amount: Amount
    currency: Currency
    description: Annotated[str, Field(min_length=1, max_length=1000, pattern=r"\S")]
    metadata: dict[str, JsonValue] = Field(default_factory=dict)
    webhook_url: HttpUrl

    @field_validator("metadata")
    @classmethod
    def validate_metadata(cls, value: dict[str, JsonValue]) -> dict[str, JsonValue]:
        # PostgreSQL JSONB rejects NaN/Infinity and the null character.
        try:
            json.dumps(value, allow_nan=False)
        except ValueError:
            raise ValueError("metadata must contain finite JSON values") from None
        if contains_null_character(value):
            raise ValueError("metadata must not contain null characters")
        return value

    @field_validator("description", "webhook_url")
    @classmethod
    def validate_text(cls, value):
        if "\x00" in str(value):
            raise ValueError("Text must not contain null characters")
        return value


class PaymentAccepted(BaseModel):
    payment_id: UUID
    status: PaymentStatus
    created_at: datetime


class PaymentDetail(PaymentAccepted):
    amount: Decimal
    currency: Currency
    description: str
    metadata: dict[str, JsonValue]
    webhook_url: str
    processed_at: datetime | None

    @field_serializer("amount")
    def serialize_amount(self, amount: Decimal) -> str:
        return format(amount, ".2f")
