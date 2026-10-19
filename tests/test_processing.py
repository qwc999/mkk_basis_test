import asyncio
from datetime import UTC, datetime, timedelta
from unittest.mock import AsyncMock
from uuid import UUID, uuid4

import pytest
from sqlalchemy import text

from app.models import Payment
from app.processing import PaymentNotFound, process_payment, simulate_payment


@pytest.mark.parametrize("result", ["succeeded", "failed"])
async def test_processing_stores_final_result(
    client, headers, payment_data, session_factory, result
):
    response = await client.post("/api/v1/payments", headers=headers, json=payment_data)
    payment_id = UUID(response.json()["payment_id"])
    simulator = AsyncMock(return_value=result)
    before = datetime.now(UTC)
    await process_payment(session_factory, payment_id, simulator)
    after = datetime.now(UTC)
    async with session_factory() as session:
        payment = await session.get(Payment, payment_id)
        assert payment.status == result
        assert before <= payment.processed_at <= after
        assert payment.processed_at.utcoffset() == timedelta(0)
        original_time = payment.processed_at
    await process_payment(session_factory, payment_id, simulator)
    simulator.assert_awaited_once()
    detail = await client.get(f"/api/v1/payments/{payment_id}", headers=headers)
    assert detail.json()["status"] == result
    assert datetime.fromisoformat(detail.json()["processed_at"]) == original_time


async def test_concurrent_processing_runs_simulator_once(
    client, headers, payment_data, session_factory
):
    response = await client.post("/api/v1/payments", headers=headers, json=payment_data)
    payment_id = UUID(response.json()["payment_id"])
    entered = asyncio.Event()
    release = asyncio.Event()
    calls = 0

    async def simulator():
        nonlocal calls
        calls += 1
        entered.set()
        await release.wait()
        return "succeeded"

    first = asyncio.create_task(process_payment(session_factory, payment_id, simulator))
    tasks = [first]
    try:
        await asyncio.wait_for(entered.wait(), 5)
        second = asyncio.create_task(process_payment(session_factory, payment_id, simulator))
        tasks.append(second)
        # Let the second connection reach the locked row before releasing the first.

        async with asyncio.timeout(5):
            while True:
                async with session_factory() as session:
                    waiting = await session.scalar(
                        text(
                            "SELECT EXISTS(SELECT 1 FROM pg_stat_activity "
                            "WHERE datname = current_database() AND wait_event_type = 'Lock' "
                            "AND query LIKE '%payments%')"
                        )
                    )
                if waiting:
                    break
                await asyncio.sleep(0.01)
        release.set()
        await asyncio.wait_for(asyncio.gather(first, second), 5)
    finally:
        release.set()
        for task in tasks:
            task.cancel()
        await asyncio.gather(*tasks, return_exceptions=True)
    assert calls == 1


@pytest.mark.parametrize("cancel", [False, True])
async def test_failed_or_cancelled_processing_rolls_back(
    client, headers, payment_data, session_factory, cancel
):
    response = await client.post("/api/v1/payments", headers=headers, json=payment_data)
    payment_id = UUID(response.json()["payment_id"])
    entered = asyncio.Event()

    async def simulator():
        entered.set()
        if cancel:
            await asyncio.Event().wait()
        raise RuntimeError("simulated technical failure")

    task = asyncio.create_task(process_payment(session_factory, payment_id, simulator))
    await asyncio.wait_for(entered.wait(), 5)
    if cancel:
        task.cancel()
    with pytest.raises(asyncio.CancelledError if cancel else RuntimeError):
        await task
    async with session_factory() as session:
        payment = await session.get(Payment, payment_id)
        assert payment.status == "pending"
        assert payment.processed_at is None
    await process_payment(session_factory, payment_id, AsyncMock(return_value="failed"))


async def test_processing_unknown_payment(session_factory):
    simulator = AsyncMock()
    with pytest.raises(PaymentNotFound):
        await process_payment(session_factory, uuid4(), simulator)
    simulator.assert_not_awaited()


@pytest.mark.parametrize(
    ("random_value", "result"),
    [(0, "succeeded"), (0.8999, "succeeded"), (0.9, "failed"), (0.9999, "failed")],
)
async def test_emulator_is_async_and_has_deterministic_boundaries(
    monkeypatch, random_value, result
):
    sleep = AsyncMock()
    monkeypatch.setattr("app.processing.asyncio.sleep", sleep)
    monkeypatch.setattr("app.processing.random.uniform", lambda low, high: 3.25)
    monkeypatch.setattr("app.processing.random.random", lambda: random_value)
    assert await simulate_payment() == result
    sleep.assert_awaited_once_with(3.25)
