"""SQLite-адаптеры: durable-хранение без внешних сервисов (профиль T1/T2)."""

from __future__ import annotations

import json
import sqlite3
import threading
import time
from pathlib import Path
from typing import Any

from agentos.contracts.storage import EventLogPort, KVStorePort, LogRecord


class _SqliteBase:
    def __init__(self, path: str | Path) -> None:
        self._conn = sqlite3.connect(str(path), check_same_thread=False)
        self._conn.execute("PRAGMA journal_mode=WAL")
        self._lock = threading.Lock()


class SqliteKV(_SqliteBase, KVStorePort):
    def __init__(self, path: str | Path) -> None:
        super().__init__(path)
        with self._lock:
            self._conn.execute(
                "CREATE TABLE IF NOT EXISTS kv ("
                " ns TEXT, key TEXT, value TEXT, expires REAL,"
                " PRIMARY KEY (ns, key))"
            )
            self._conn.commit()

    async def get(self, ns: str, key: str) -> Any | None:
        with self._lock:
            row = self._conn.execute(
                "SELECT value, expires FROM kv WHERE ns=? AND key=?", (ns, key)
            ).fetchone()
        if row is None:
            return None
        value, expires = row
        if expires is not None and expires <= time.time():
            await self.delete(ns, key)
            return None
        return json.loads(value)

    async def put(self, ns: str, key: str, value: Any, *, ttl: float | None = None) -> None:
        expires = time.time() + ttl if ttl else None
        with self._lock:
            self._conn.execute(
                "INSERT OR REPLACE INTO kv (ns, key, value, expires) VALUES (?,?,?,?)",
                (ns, key, json.dumps(value, ensure_ascii=False), expires),
            )
            self._conn.commit()

    async def delete(self, ns: str, key: str) -> bool:
        with self._lock:
            cur = self._conn.execute(
                "DELETE FROM kv WHERE ns=? AND key=?", (ns, key)
            )
            self._conn.commit()
        return cur.rowcount > 0

    async def keys(self, ns: str, prefix: str = "") -> list[str]:
        now = time.time()
        with self._lock:
            rows = self._conn.execute(
                "SELECT key FROM kv WHERE ns=? AND key LIKE ?"
                " AND (expires IS NULL OR expires > ?)",
                (ns, prefix + "%", now),
            ).fetchall()
        return [r[0] for r in rows]


class SqliteEventLog(_SqliteBase, EventLogPort):
    """Durable Event Log — история событий, replay, аудит."""

    def __init__(self, path: str | Path) -> None:
        super().__init__(path)
        with self._lock:
            self._conn.execute(
                "CREATE TABLE IF NOT EXISTS event_log ("
                " offset INTEGER PRIMARY KEY AUTOINCREMENT,"
                " topic TEXT, time REAL, data TEXT)"
            )
            self._conn.commit()

    async def append(self, topic: str, data: dict[str, Any]) -> int:
        with self._lock:
            cur = self._conn.execute(
                "INSERT INTO event_log (topic, time, data) VALUES (?,?,?)",
                (topic, time.time(), json.dumps(data, ensure_ascii=False)),
            )
            self._conn.commit()
        return int(cur.lastrowid or 0)

    async def read(
        self, *, since_offset: int = 0, since_time: float = 0.0, limit: int = 1000
    ) -> list[LogRecord]:
        with self._lock:
            rows = self._conn.execute(
                "SELECT offset, topic, time, data FROM event_log"
                " WHERE offset >= ? AND time >= ? ORDER BY offset LIMIT ?",
                (since_offset, since_time, limit),
            ).fetchall()
        return [LogRecord(o, t, tm, json.loads(d)) for o, t, tm, d in rows]
