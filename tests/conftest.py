import os
import subprocess
import sys
from pathlib import Path
from urllib.parse import unquote, urlsplit

import httpx
import pytest
from sqlalchemy import text
from sqlalchemy.engine import make_url

from app.config import Settings, WorkerSettings
from app.db import create_engine, create_session_factory
from app.main import create_app
from app.messaging import create_broker, declare_topology

TEST_API_KEY = "test-only-api-key"


def validate_test_database_url(raw_url: str) -> str:
    url = make_url(raw_url)
    if (
        url.drivername != "postgresql+asyncpg"
        or url.database != "payments_test"
        or url.username != "payments_test"
        or not url.host
    ):
        raise ValueError("Tests require PostgreSQL database AND user named payments_test")
    return raw_url


@pytest.fixture(scope="session")
def database_url():
    raw_url = os.environ.get("TEST_DATABASE_URL")
    if not raw_url:
        pytest.fail("Set TEST_DATABASE_URL to a dedicated payments_test database; see README")
    return validate_test_database_url(raw_url)


@pytest.fixture(scope="session")
def migrated_database(database_url):
    env = {**os.environ, "DATABASE_URL": database_url, "API_KEY": TEST_API_KEY}
    subprocess.run(
        [sys.executable, "-m", "alembic", "upgrade", "head"],
        cwd=Path(__file__).resolve().parents[1],
        env=env,
        check=True,
    )


@pytest.fixture
def settings(database_url):
    return Settings(database_url=database_url, api_key=TEST_API_KEY, _env_file=None)


@pytest.fixture
async def engine(settings, migrated_database):
    engine = create_engine(settings)
    async with engine.begin() as connection:
        await connection.execute(text("TRUNCATE TABLE outbox, payments"))
    try:
        yield engine
    finally:
        await engine.dispose()


@pytest.fixture
def session_factory(engine):
    return create_session_factory(engine)


@pytest.fixture
async def client(settings, engine):
    app = create_app(settings)
    async with app.router.lifespan_context(app):
        transport = httpx.ASGITransport(app=app, raise_app_exceptions=False)
        async with httpx.AsyncClient(transport=transport, base_url="http://test") as client:
            yield client


@pytest.fixture
def headers():
    return {"X-API-Key": TEST_API_KEY, "Idempotency-Key": "order-123"}


@pytest.fixture
def payment_data():
    return {
        "amount": "1500.00",
        "currency": "RUB",
        "description": "Оплата заказа",
        "metadata": {"order_id": "order-123"},
        "webhook_url": "https://example.com/webhooks/payments",
    }


def validate_test_rabbitmq_url(raw_url: str) -> str:
    url = urlsplit(raw_url)
    if (
        url.scheme != "amqp"
        or url.username != "payments_test"
        or unquote(url.path) != "/payments_test"
        or not url.hostname
    ):
        raise ValueError("Tests require RabbitMQ user AND vhost named payments_test")
    return raw_url


@pytest.fixture
def worker_settings(database_url):
    raw_url = os.environ.get("TEST_RABBITMQ_URL")
    if not raw_url:
        pytest.fail("Set TEST_RABBITMQ_URL to the dedicated payments_test vhost; see README")
    return WorkerSettings(
        database_url=database_url,
        rabbitmq_url=validate_test_rabbitmq_url(raw_url),
        outbox_poll_interval=0.05,
        outbox_publish_timeout=1,
        _env_file=None,
    )


@pytest.fixture
async def rabbit(worker_settings):
    broker = create_broker(worker_settings)
    try:
        await broker.connect()
        queue = await declare_topology(broker)
        await queue.purge()
        yield broker, queue
    finally:
        await broker.stop()
