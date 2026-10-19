import asyncio
import logging
import random
from collections.abc import Awaitable, Callable
from datetime import UTC, datetime
from typing import Literal
from uuid import UUID

from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker

from app.models import Payment

logger = logging.getLogger(__name__)
FinalStatus = Literal["succeeded", "failed"]
Simulator = Callable[[], Awaitable[FinalStatus]]


class PaymentNotFound(Exception):
    pass


async def simulate_payment() -> FinalStatus:
    await asyncio.sleep(random.uniform(2, 5))
    return "succeeded" if random.random() < 0.9 else "failed"


async def process_payment(
    sessions: async_sessionmaker[AsyncSession],
    payment_id: UUID,
    simulator: Simulator = simulate_payment,
) -> None:
    async with sessions.begin() as session:
        payment = await session.scalar(
            select(Payment).where(Payment.id == payment_id).with_for_update()
        )
        if payment is None:
            raise PaymentNotFound
        if payment.status == "pending":
            result = await simulator()
            if result not in {"succeeded", "failed"}:
                raise ValueError("Simulator returned an invalid final status")
            payment.status = result
            payment.processed_at = datetime.now(UTC)
    logger.info("Payment processing committed: payment_id=%s", payment_id)
