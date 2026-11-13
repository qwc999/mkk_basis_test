import secrets
from typing import Annotated
from uuid import UUID

from fastapi import APIRouter, Depends, Header, HTTPException, Request, status
from fastapi.security import APIKeyHeader

from app.application.payments import create_payment
from app.application.payments import get_payment as read_payment
from app.application.ports import UnitOfWorkFactory
from app.domain.errors import IdempotencyConflict, PaymentNotFound
from app.domain.models import Payment, PaymentDraft
from app.presentation.api.json_request import PaymentJSONRoute
from app.presentation.api.schemas import (
    IdempotencyKey,
    PaymentAccepted,
    PaymentCreate,
    PaymentDetail,
)

api_key_header = APIKeyHeader(name="X-API-Key", auto_error=False)


def authenticate(request: Request, api_key: Annotated[str | None, Depends(api_key_header)]) -> None:
    expected = request.app.state.settings.api_key.get_secret_value()
    if api_key is None or not secrets.compare_digest(api_key.encode(), expected.encode()):
        raise HTTPException(status_code=401, detail="Invalid or missing API key")


router = APIRouter(
    prefix="/api/v1/payments",
    dependencies=[Depends(authenticate)],
    route_class=PaymentJSONRoute,
)


def get_uows(request: Request) -> UnitOfWorkFactory:
    return request.app.state.uows


Uows = Annotated[UnitOfWorkFactory, Depends(get_uows)]


def accepted(payment: Payment) -> PaymentAccepted:
    return PaymentAccepted(
        payment_id=payment.id, status=payment.status, created_at=payment.created_at
    )


@router.post("", response_model=PaymentAccepted, status_code=status.HTTP_202_ACCEPTED)
async def post_payment(
    data: PaymentCreate,
    uows: Uows,
    idempotency_key: Annotated[IdempotencyKey, Header()],
) -> PaymentAccepted:
    if "\x00" in idempotency_key:
        raise HTTPException(status_code=422, detail="Invalid Idempotency-Key")
    try:
        payment = await create_payment(
            uows,
            PaymentDraft(
                data.amount, data.currency, data.description, data.metadata, str(data.webhook_url)
            ),
            idempotency_key,
        )
    except IdempotencyConflict:
        raise HTTPException(
            status_code=409, detail="Idempotency-Key was used with different data"
        ) from None
    return accepted(payment)


@router.get("/{payment_id}", response_model=PaymentDetail)
async def get_payment(payment_id: UUID, uows: Uows) -> PaymentDetail:
    try:
        payment = await read_payment(uows, payment_id)
    except PaymentNotFound:
        raise HTTPException(status_code=404, detail="Payment not found") from None
    return PaymentDetail(
        **accepted(payment).model_dump(),
        amount=payment.amount,
        currency=payment.currency,
        description=payment.description,
        metadata=payment.metadata,
        webhook_url=payment.webhook_url,
        processed_at=payment.processed_at,
    )
