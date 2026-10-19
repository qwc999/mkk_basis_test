import pytest
from pydantic import ValidationError

from app.config import Settings, WorkerSettings
from tests.conftest import validate_test_rabbitmq_url

DB = "postgresql+asyncpg://payments_test:test-only@postgres/payments_test"
MQ = "amqp://payments_test:test-only@rabbitmq/payments_test"


def test_worker_does_not_require_api_key(monkeypatch):
    monkeypatch.delenv("API_KEY", raising=False)
    settings = WorkerSettings(database_url=DB, rabbitmq_url=MQ, _env_file=None)
    assert not hasattr(settings, "api_key")


def test_api_does_not_require_rabbitmq(monkeypatch):
    monkeypatch.delenv("RABBITMQ_URL", raising=False)
    assert Settings(database_url=DB, api_key="test-only", _env_file=None)


@pytest.mark.parametrize("value", [0, -1, float("nan"), float("inf"), 61])
@pytest.mark.parametrize("field", ["outbox_poll_interval", "outbox_publish_timeout"])
def test_worker_intervals_are_bounded(value, field):
    with pytest.raises(ValidationError):
        WorkerSettings(database_url=DB, rabbitmq_url=MQ, **{field: value}, _env_file=None)


@pytest.mark.parametrize("url", ["http://user:secret@example.com/", "amqp:///", "broken"])
def test_worker_rejects_invalid_broker_url_without_disclosing_it(url):
    with pytest.raises(ValidationError) as exc:
        WorkerSettings(database_url=DB, rabbitmq_url=url, _env_file=None)
    assert url not in str(exc.value)


@pytest.mark.parametrize(
    "url",
    ["amqp://payments:secret@rabbitmq/payments", "amqp://payments_test:secret@rabbitmq/payments"],
)
def test_guard_rejects_working_broker(url):
    with pytest.raises(ValueError, match="payments_test"):
        validate_test_rabbitmq_url(url)
