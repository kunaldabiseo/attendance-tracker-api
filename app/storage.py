from __future__ import annotations

import json
import logging
from pathlib import Path
from typing import Dict, List, Optional

from .config import settings
from .utils import dump_json, load_json

logger = logging.getLogger(__name__)

_SCHEMA_SQL = """
CREATE TABLE IF NOT EXISTS attendance_uploads (
    id INTEGER PRIMARY KEY CHECK (id = 1),
    checkin_json TEXT NOT NULL DEFAULT '[]',
    checkout_json TEXT NOT NULL DEFAULT '[]',
    breaks_json TEXT NOT NULL DEFAULT '[]'
);
INSERT OR IGNORE INTO attendance_uploads (id, checkin_json, checkout_json, breaks_json)
VALUES (1, '[]', '[]', '[]');
"""


def _get_turso_store() -> "TursoStore":
    return TursoStore()


class DataStore:
    """
    Storage for uploaded CSV rows. Uses Turso (persistent) when configured,
    otherwise file-based (ephemeral on Render).
    """

    def __init__(self, cache_dir: Optional[Path] = None):
        self.cache_dir = cache_dir or settings.cache_dir
        self.cache_path = self.cache_dir / "uploads.json"
        self._payload: Dict[str, List[dict]] = {"checkin": [], "checkout": [], "breaks": []}
        self._loaded = False
        self._use_turso = settings.use_turso
        self._turso: Optional["TursoStore"] = None

    def _ensure_turso(self) -> "TursoStore":
        if self._turso is None:
            self._turso = _get_turso_store()
        return self._turso

    def load(self) -> None:
        if self._loaded:
            return
        if self._use_turso:
            try:
                t = self._ensure_turso()
                data = t.load()
                for key in self._payload:
                    self._payload[key] = data.get(key, [])
                logger.info("Loaded attendance data from Turso")
            except Exception as exc:  # pylint: disable=broad-except
                logger.warning("Turso load failed, using empty: %s", exc)
        elif self.cache_path.exists():
            try:
                data = load_json(self.cache_path)
                for key in self._payload:
                    self._payload[key] = data.get(key, [])
                logger.info("Loaded cached uploads from %s", self.cache_path)
            except Exception as exc:  # pylint: disable=broad-except
                logger.warning("Failed to load cache %s: %s", self.cache_path, exc)
        self._loaded = True

    def save(self) -> None:
        if self._use_turso:
            try:
                self._ensure_turso().save(self._payload)
            except Exception as exc:  # pylint: disable=broad-except
                logger.error("Turso save failed: %s", exc)
        else:
            try:
                dump_json(self.cache_path, self._payload)
            except Exception as exc:  # pylint: disable=broad-except
                logger.error("Failed to write cache %s: %s", self.cache_path, exc)

    def update(self, key: str, rows: List[dict]) -> None:
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
        self._loaded = True
        self.save()
        logger.info("Storage cleared - all data reset")


class TursoStore:
    """Persistent storage using Turso/libSQL."""

    def __init__(self) -> None:
        self._url = settings.turso_database_url
        self._auth_token = settings.turso_auth_token
        if not self._url or not self._auth_token:
            raise ValueError("TURSO_DATABASE_URL and TURSO_AUTH_TOKEN required for Turso storage")

    def _conn(self):
        import libsql_client

        return libsql_client.create_client_sync(
            url=self._url,
            auth_token=self._auth_token,
        )

    def _init_schema(self, client) -> None:
        for stmt in _SCHEMA_SQL.strip().split(";"):
            stmt = stmt.strip()
            if stmt:
                client.execute(stmt)

    def load(self) -> Dict[str, List[dict]]:
        with self._conn() as client:
            self._init_schema(client)
            rs = client.execute("SELECT checkin_json, checkout_json, breaks_json FROM attendance_uploads WHERE id = 1")
            if not rs.rows:
                return {"checkin": [], "checkout": [], "breaks": []}
            row = rs.rows[0]
            return {
                "checkin": json.loads(row[0]) if row[0] else [],
                "checkout": json.loads(row[1]) if row[1] else [],
                "breaks": json.loads(row[2]) if row[2] else [],
            }

    def save(self, payload: Dict[str, List[dict]]) -> None:
        checkin_json = json.dumps(payload.get("checkin", []), default=str)
        checkout_json = json.dumps(payload.get("checkout", []), default=str)
        breaks_json = json.dumps(payload.get("breaks", []), default=str)
        with self._conn() as client:
            self._init_schema(client)
            client.execute(
                """
                INSERT OR REPLACE INTO attendance_uploads (id, checkin_json, checkout_json, breaks_json)
                VALUES (1, ?, ?, ?)
                """,
                [checkin_json, checkout_json, breaks_json],
            )


store = DataStore()
