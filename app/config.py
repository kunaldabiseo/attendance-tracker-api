from __future__ import annotations

import os
from pathlib import Path
from typing import Optional

from pydantic import Field, model_validator
from pydantic_settings import BaseSettings, SettingsConfigDict


class Settings(BaseSettings):
    """Runtime configuration for the backend."""

    model_config = SettingsConfigDict(
        env_prefix="KD_TRACKER_",
        extra="ignore",
    )

    app_name: str = "KD Digital Attendance & Break Tracker"
    cache_dir: Path = Field(default_factory=lambda: Path(__file__).resolve().parent.parent / "data" / ".cache")
    timezone: str = "Asia/Kolkata"

    # Turso (persistent storage) - when set, data survives restarts on Render
    turso_database_url: Optional[str] = Field(default=None, description="Turso libsql URL")
    turso_auth_token: Optional[str] = Field(default=None, description="Turso auth token")

    @model_validator(mode="after")
    def _fallback_turso_env(self) -> "Settings":
        if not self.turso_database_url:
            self.turso_database_url = os.getenv("TURSO_DATABASE_URL")
        if not self.turso_auth_token:
            self.turso_auth_token = os.getenv("TURSO_AUTH_TOKEN")
        return self

    @property
    def use_turso(self) -> bool:
        return bool(self.turso_database_url and self.turso_auth_token)


settings = Settings()
settings.cache_dir.mkdir(parents=True, exist_ok=True)


