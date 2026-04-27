from pydantic import model_validator
from pydantic_settings import BaseSettings, SettingsConfigDict


class Settings(BaseSettings):
    model_config = SettingsConfigDict(env_file=".env", extra="ignore")

    database_url: str | None = None
    db_host: str = "localhost"
    db_port: int = 5432
    db_name: str = "db"
    db_user: str = "db_user"
    db_password: str = "db_password"
    jwt_secret: str = "change-me-in-env"
    jwt_algorithm: str = "HS256"
    access_token_minutes: int = 120
    api_prefix: str = "/api/v1"
    legacy_schema: str = "dev"
    bootstrap_from_legacy: bool = True
    allow_sqlite_fallback: bool = False

    @model_validator(mode="after")
    def _resolve_database_url(self) -> "Settings":
        if not self.database_url:
            host = "localhost" if self.db_host == "db" else self.db_host
            self.database_url = (
                f"postgresql+asyncpg://{self.db_user}:{self.db_password}"
                f"@{host}:{self.db_port}/{self.db_name}"
            )
        weak_secrets = {"", "change-me-in-env", "change-me-in-production", "your-secret-key", "secret"}
        if (self.jwt_secret or "").strip().lower() in weak_secrets:
            raise ValueError("JWT_SECRET is weak or missing. Set a strong value in environment variables.")
        return self


settings = Settings()
