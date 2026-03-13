from __future__ import annotations

from pathlib import Path

from pydantic import Field
from pydantic_settings import BaseSettings, SettingsConfigDict


class Settings(BaseSettings):
    """Runtime configuration for the backend."""

    model_config = SettingsConfigDict(env_prefix="KD_TRACKER_")

    app_name: str = "KD Digital Attendance & Break Tracker"
    cache_dir: Path = Field(default_factory=lambda: Path(__file__).resolve().parent.parent / "data" / ".cache")
    timezone: str = "Asia/Kolkata"


settings = Settings()
settings.cache_dir.mkdir(parents=True, exist_ok=True)


