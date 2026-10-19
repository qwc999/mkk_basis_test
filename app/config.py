from pydantic import SecretStr, ValidationError, field_validator
from pydantic_settings import BaseSettings, SettingsConfigDict
from sqlalchemy.engine import make_url
from sqlalchemy.exc import ArgumentError


class Settings(BaseSettings):
    model_config = SettingsConfigDict(env_file=".env", extra="ignore", hide_input_in_errors=True)

    database_url: SecretStr
    api_key: SecretStr

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

    @field_validator("api_key")
    @classmethod
    def validate_api_key(cls, value: SecretStr) -> SecretStr:
        if not value.get_secret_value().strip():
            raise ValueError("API_KEY must not be blank")
        return value


def load_settings() -> Settings:
    try:
        return Settings()
    except ValidationError as exc:
        # Report field names only: validation inputs can contain credentials.
        fields = ", ".join(sorted({str(error["loc"][0]).upper() for error in exc.errors()}))
        raise RuntimeError(
            f"Missing or invalid configuration: {fields}. Check .env and .env.example."
        ) from None
