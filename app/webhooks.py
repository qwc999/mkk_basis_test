import asyncio
from datetime import UTC, datetime
from uuid import UUID

import httpx
from sqlalchemy import update
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker

from app.models import Payment
from app.processing import PaymentNotFound

HTTP_TIMEOUT = 5.0


class WebhookDeliveryError(Exception):
    pass


def create_http_client() -> httpx.AsyncClient:
    return httpx.AsyncClient(timeout=HTTP_TIMEOUT, follow_redirects=False)


async def deliver_webhook(
    sessions: async_sessionmaker[AsyncSession], payment_id: UUID, client: httpx.AsyncClient
) -> None:
    async with sessions() as session:
        payment = await session.get(Payment, payment_id)
        if payment is None:
            raise PaymentNotFound
        if payment.webhook_delivered_at is not None:
            return
        if payment.status not in {"succeeded", "failed"} or payment.processed_at is None:
            raise WebhookDeliveryError
        payload = {
            "payment_id": str(payment.id),
            "status": payment.status,
            "amount": format(payment.amount, ".2f"),
            "currency": payment.currency,
            "processed_at": payment.processed_at.astimezone(UTC).isoformat(),
        }
        url = payment.webhook_url
    # Reading is complete: no session, transaction or row lock spans the HTTP request.
    async with asyncio.timeout(HTTP_TIMEOUT):
        response = await client.post(
            url,
            json=payload,
            headers={"Idempotency-Key": str(payment_id)},
            timeout=HTTP_TIMEOUT,
            follow_redirects=False,
        )
    if not response.is_success:
        raise WebhookDeliveryError
    async with sessions.begin() as session:
        result = await session.execute(
            update(Payment)
            .where(Payment.id == payment_id)
            .values(webhook_delivered_at=datetime.now(UTC))
        )
        if result.rowcount != 1:
            raise PaymentNotFound
