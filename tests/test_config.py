import pytest
from pydantic import ValidationError

from app.config import Settings, load_settings
from tests.conftest import validate_test_database_url


def test_missing_settings_fail_clearly(monkeypatch, tmp_path):
    monkeypatch.chdir(tmp_path)
    monkeypatch.delenv("DATABASE_URL", raising=False)
    monkeypatch.delenv("API_KEY", raising=False)
    with pytest.raises(RuntimeError, match="API_KEY, DATABASE_URL"):
        load_settings()


@pytest.mark.parametrize("url", ["sqlite:///local.db", "not-a-url", "postgresql://u:p@db/x"])
def test_invalid_database_configuration_is_redacted(url):
    with pytest.raises(ValidationError) as exc:
        Settings(database_url=url, api_key="secret-api-key", _env_file=None)
    assert url not in str(exc.value)
    assert "secret-api-key" not in str(exc.value)


@pytest.mark.parametrize("key", ["", " ", "\t"])
def test_empty_api_key_is_rejected(key):
    with pytest.raises(ValidationError):
        Settings(database_url="postgresql+asyncpg://user:password@db/payments", api_key=key)


def test_configuration_error_does_not_disclose_secrets(monkeypatch, tmp_path):
    monkeypatch.chdir(tmp_path)
    monkeypatch.setenv("DATABASE_URL", "invalid-url-containing-secret")
    monkeypatch.setenv("API_KEY", "secret-api-key")
    with pytest.raises(RuntimeError) as exc:
        load_settings()
    assert "DATABASE_URL" in str(exc.value)
    assert "secret" not in str(exc.value)


@pytest.mark.parametrize(
    "url",
    [
        "postgresql+asyncpg://payments:password@db/payments",
        "postgresql+asyncpg://payments:password@db/payments_test",
        "postgresql+asyncpg://payments_test:password@db/payments",
        "sqlite:///payments_test",
    ],
)
def test_test_database_guard_rejects_working_database(url):
    with pytest.raises(ValueError, match="payments_test"):
        validate_test_database_url(url)
