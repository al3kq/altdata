"""Application settings loaded from environment variables and .env file."""

from __future__ import annotations

from pathlib import Path
from typing import Literal

from pydantic import Field
from pydantic_settings import BaseSettings, SettingsConfigDict


class Settings(BaseSettings):
    """Central configuration for the altdata framework.

    All settings can be overridden via environment variables or a .env file.
    """

    # Database
    database_url: str = "postgresql+asyncpg://postgres:postgres@localhost:5432/altdata"

    # Raw storage
    raw_store_backend: Literal["disk", "s3"] = "disk"
    raw_store_path: Path = Field(default_factory=lambda: Path("~/.altdata/raw").expanduser())
    s3_bucket: str = ""
    s3_prefix: str = "raw/"

    # Oxylabs proxy
    oxylabs_username: str = ""
    oxylabs_password: str = ""
    oxylabs_endpoint: str = "pr.oxylabs.io:7777"

    # HTTP client
    http_timeout: float = 30.0
    http_max_retries: int = 3
    http_backoff_factor: float = 2.0

    # Playwright
    playwright_headless: bool = True

    model_config = SettingsConfigDict(
        env_file=".env",
        env_file_encoding="utf-8",
        case_sensitive=False,
    )


# Module-level singleton — lazily instantiated
_settings: Settings | None = None


def get_settings() -> Settings:
    """Return the cached Settings singleton."""
    global _settings
    if _settings is None:
        _settings = Settings()
    return _settings
