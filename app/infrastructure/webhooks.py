import asyncio
from datetime import UTC

import httpx

from app.application.errors import WebhookDeliveryError
from app.domain.models import Payment

HTTP_TIMEOUT = 5.0


def create_http_client() -> httpx.AsyncClient:
    return httpx.AsyncClient(timeout=HTTP_TIMEOUT, follow_redirects=False)


class HttpxWebhookSender:
    def __init__(self, client: httpx.AsyncClient):
        self.client = client

    async def send(self, payment: Payment) -> None:
        try:
            async with asyncio.timeout(HTTP_TIMEOUT):
                response = await self.client.post(
                    payment.webhook_url,
                    json={
                        "payment_id": str(payment.id),
                        "status": payment.status,
                        "amount": format(payment.amount, ".2f"),
                        "currency": payment.currency,
                        "processed_at": payment.processed_at.astimezone(UTC).isoformat(),
                    },
                    headers={"Idempotency-Key": str(payment.id)},
                    timeout=HTTP_TIMEOUT,
                    follow_redirects=False,
                )
        except (httpx.HTTPError, TimeoutError) as exc:
            raise WebhookDeliveryError from exc
        if not response.is_success:
            raise WebhookDeliveryError
