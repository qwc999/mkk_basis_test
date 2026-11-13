import asyncio
import random

from app.domain.models import FinalStatus, Payment


async def simulate_payment() -> FinalStatus:
    await asyncio.sleep(random.uniform(2, 5))
    return "succeeded" if random.random() < 0.9 else "failed"


class SimulatedPaymentGateway:
    async def charge(self, payment: Payment) -> FinalStatus:
        return await simulate_payment()
