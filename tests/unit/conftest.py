from decimal import Decimal

import pytest

from app.domain.models import PaymentDraft
from tests.unit.fakes import MemoryStore


@pytest.fixture
def store():
    return MemoryStore()


@pytest.fixture
def draft():
    return PaymentDraft(Decimal("1500.00"), "RUB", "Order", {"paid": True}, "https://test/hook")
