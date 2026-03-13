from __future__ import annotations

import logging
from pathlib import Path
from typing import Dict, List, Optional

from .config import settings
from .utils import dump_json, load_json

logger = logging.getLogger(__name__)


class DataStore:
    """
    Simple on-disk cache for uploaded CSV rows.

    Stored payload structure:
    {
        "checkin": [...],
        "checkout": [...],
        "breaks": [...]
    }
    """

    def __init__(self, cache_dir: Optional[Path] = None):
        self.cache_dir = cache_dir or settings.cache_dir
        self.cache_path = self.cache_dir / "uploads.json"
        self._payload: Dict[str, List[dict]] = {"checkin": [], "checkout": [], "breaks": []}
        self._loaded = False

    def load(self) -> None:
        if self._loaded:
            return
        if self.cache_path.exists():
            try:
                data = load_json(self.cache_path)
                for key in self._payload:
                    self._payload[key] = data.get(key, [])
                logger.info("Loaded cached uploads from %s", self.cache_path)
            except Exception as exc:  # pylint: disable=broad-except
                logger.warning("Failed to load cache %s: %s", self.cache_path, exc)
        self._loaded = True

    def save(self) -> None:
        try:
            dump_json(self.cache_path, self._payload)
        except Exception as exc:  # pylint: disable=broad-except
            logger.error("Failed to write cache %s: %s", self.cache_path, exc)

    def update(self, key: str, rows: List[dict]) -> None:
        # Don't reload if we just cleared - prevents reloading old data
        if not self._loaded:
            self.load()
        self._payload[key] = rows
        self.save()

    def extend(self, key: str, rows: List[dict]) -> None:
        self.load()
        self._payload[key].extend(rows)
        self.save()

    def get(self, key: str) -> List[dict]:
        self.load()
        return list(self._payload.get(key, []))

    def clear(self) -> None:
        self._payload = {"checkin": [], "checkout": [], "breaks": []}
        self._loaded = True  # Mark as loaded so update() won't reload old data
        self.save()
        logger.info("Storage cleared - all data reset")


store = DataStore()













