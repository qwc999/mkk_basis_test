from urllib.parse import urlsplit

from pydantic import SecretStr, ValidationError, field_validator
from pydantic_settings import BaseSettings, SettingsConfigDict
from sqlalchemy.engine import make_url
from sqlalchemy.exc import ArgumentError


class DatabaseSettings(BaseSettings):
    model_config = SettingsConfigDict(env_file=".env", extra="ignore", hide_input_in_errors=True)

    database_url: SecretStr

    @field_validator("database_url")
    @classmethod
    def validate_database_url(cls, value: SecretStr) -> SecretStr:
        try:
            url = make_url(value.get_secret_value())
            valid = (
                url.drivername == "postgresql+asyncpg"
                and url.host
                and url.database
                and url.username
            )
        except ArgumentError:
            valid = False
        if not valid:
            raise ValueError("Expected a postgresql+asyncpg URL with host, database and username")
        return value


class Settings(DatabaseSettings):
    api_key: SecretStr
    enable_docs: bool = False

    @field_validator("api_key")
    @classmethod
    def validate_api_key(cls, value: SecretStr) -> SecretStr:
        if not value.get_secret_value().strip():
            raise ValueError("API_KEY must not be blank")
        return value


class WorkerSettings(DatabaseSettings):
    rabbitmq_url: SecretStr
    outbox_poll_interval: float = 1.0
    outbox_publish_timeout: float = 5.0

    @field_validator("rabbitmq_url")
    @classmethod
    def validate_rabbitmq_url(cls, value: SecretStr) -> SecretStr:
        try:
            url = urlsplit(value.get_secret_value())
            valid = (
                url.scheme in {"amqp", "amqps"}
                and url.hostname
                and (url.port is None or url.port > 0)
            )
        except ValueError:
            valid = False
        if not valid:
            raise ValueError("Expected an amqp or amqps URL with a host")
        return value

    @field_validator("outbox_poll_interval", "outbox_publish_timeout")
    @classmethod
    def validate_interval(cls, value: float) -> float:
        if not 0 < value <= 60:
            raise ValueError("Expected a finite interval in (0, 60] seconds")
        return value


def load_settings[T: DatabaseSettings](settings_type: type[T] = Settings) -> T:
    try:
        return settings_type()
    except ValidationError as exc:
        # Report field names only: validation inputs can contain credentials.
        fields = ", ".join(sorted({str(error["loc"][0]).upper() for error in exc.errors()}))
        raise RuntimeError(
            f"Missing or invalid configuration: {fields}. Check .env and .env.example."
        ) from None
