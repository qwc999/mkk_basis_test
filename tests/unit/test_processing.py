from unittest.mock import AsyncMock

import pytest

from app.application.payments import create_payment
from app.application.ports import PaymentGateway
from app.application.processing import process_payment
from app.domain.errors import InvalidPaymentState


@pytest.mark.parametrize("result", ["succeeded", "failed"])
async def test_gateway_is_injected_and_terminal_result_is_preserved(store, draft, result):
    payment = await create_payment(store, draft, "key")
    gateway = AsyncMock(spec=PaymentGateway)
    gateway.charge.return_value = result
    await process_payment(store, payment.id, gateway)
    final = store.payments[payment.id]
    assert final.status == result and final.processed_at is not None
    await process_payment(store, payment.id, gateway)
    assert store.payments[payment.id] == final
    gateway.charge.assert_awaited_once_with(payment)


@pytest.mark.parametrize("failure", ["exception", "invalid_result"])
async def test_gateway_failure_leaves_pending_state(store, draft, failure):
    payment = await create_payment(store, draft, "key")
    gateway = AsyncMock(spec=PaymentGateway)
    if failure == "exception":
        gateway.charge.side_effect = RuntimeError
        expected = RuntimeError
    else:
        gateway.charge.return_value = "processing"
        expected = InvalidPaymentState
    with pytest.raises(expected):
        await process_payment(store, payment.id, gateway)
    assert store.payments[payment.id] == payment and store.active == 0
