import secrets
from typing import Annotated
from uuid import UUID

from fastapi import APIRouter, Depends, Header, HTTPException, Request, status
from fastapi.security import APIKeyHeader
from sqlalchemy.ext.asyncio import AsyncSession

from app.db import get_session
from app.json_request import PaymentJSONRoute
from app.models import Payment
from app.payments import IdempotencyConflict, create_payment
from app.schemas import IdempotencyKey, PaymentAccepted, PaymentCreate, PaymentDetail

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
Session = Annotated[AsyncSession, Depends(get_session)]


def accepted(payment: Payment) -> PaymentAccepted:
    return PaymentAccepted(
        payment_id=payment.id, status=payment.status, created_at=payment.created_at
    )


@router.post("", response_model=PaymentAccepted, status_code=status.HTTP_202_ACCEPTED)
async def post_payment(
    data: PaymentCreate,
    session: Session,
    idempotency_key: Annotated[IdempotencyKey, Header()],
) -> PaymentAccepted:
    if "\x00" in idempotency_key:
        raise HTTPException(status_code=422, detail="Invalid Idempotency-Key")
    try:
        payment = await create_payment(session, data, idempotency_key)
    except IdempotencyConflict:
        raise HTTPException(
            status_code=409, detail="Idempotency-Key was used with different data"
        ) from None
    return accepted(payment)


@router.get("/{payment_id}", response_model=PaymentDetail)
async def get_payment(payment_id: UUID, session: Session) -> PaymentDetail:
    payment = await session.get(Payment, payment_id)
    if payment is None:
        raise HTTPException(status_code=404, detail="Payment not found")
    return PaymentDetail(
        **accepted(payment).model_dump(),
        amount=payment.amount,
        currency=payment.currency,
        description=payment.description,
        metadata=payment.payment_metadata,
        webhook_url=payment.webhook_url,
        processed_at=payment.processed_at,
    )
