"""In-memory адаптеры портов хранения (профиль T1 и тесты)."""

from __future__ import annotations

import math
import time
from typing import Any, Sequence

from agentos.contracts.storage import (
    EventLogPort,
    KVStorePort,
    LogRecord,
    VectorHit,
    VectorItem,
    VectorStorePort,
)


class InMemoryKV(KVStorePort):
    def __init__(self) -> None:
        self._data: dict[tuple[str, str], tuple[Any, float | None]] = {}

    def _alive(self, entry: tuple[Any, float | None]) -> bool:
        _, expires = entry
        return expires is None or expires > time.time()

    async def get(self, ns: str, key: str) -> Any | None:
        entry = self._data.get((ns, key))
        if entry is None or not self._alive(entry):
            self._data.pop((ns, key), None)
            return None
        return entry[0]

    async def put(self, ns: str, key: str, value: Any, *, ttl: float | None = None) -> None:
        expires = time.time() + ttl if ttl else None
        self._data[(ns, key)] = (value, expires)

    async def delete(self, ns: str, key: str) -> bool:
        return self._data.pop((ns, key), None) is not None

    async def keys(self, ns: str, prefix: str = "") -> list[str]:
        return [
            k
            for (n, k), entry in list(self._data.items())
            if n == ns and k.startswith(prefix) and self._alive(entry)
        ]


class InMemoryEventLog(EventLogPort):
    def __init__(self, retention: int = 100_000) -> None:
        self._records: list[LogRecord] = []
        self._retention = retention
        self._next_offset = 0

    async def append(self, topic: str, data: dict[str, Any]) -> int:
        offset = self._next_offset
        self._next_offset += 1
        self._records.append(LogRecord(offset, topic, time.time(), data))
        if len(self._records) > self._retention:
            self._records = self._records[-self._retention :]
        return offset

    async def read(
        self, *, since_offset: int = 0, since_time: float = 0.0, limit: int = 1000
    ) -> list[LogRecord]:
        out = [
            r
            for r in self._records
            if r.offset >= since_offset and r.time >= since_time
        ]
        return out[:limit]


def cosine(a: Sequence[float], b: Sequence[float]) -> float:
    dot = sum(x * y for x, y in zip(a, b))
    na = math.sqrt(sum(x * x for x in a))
    nb = math.sqrt(sum(y * y for y in b))
    if na == 0 or nb == 0:
        return 0.0
    return dot / (na * nb)


class InMemoryVectorStore(VectorStorePort):
    """Линейный поиск по косинусной близости — эталонная реализация
    контракта; продакшн-индексы (HNSW и т.п.) — адаптеры-плагины."""

    def __init__(self) -> None:
        self._items: dict[tuple[str, str], VectorItem] = {}

    async def upsert(self, ns: str, items: Sequence[VectorItem]) -> None:
        for item in items:
            self._items[(ns, item.id)] = item

    async def search(
        self,
        ns: str,
        vector: Sequence[float],
        *,
        k: int = 10,
        where: dict[str, Any] | None = None,
    ) -> list[VectorHit]:
        hits: list[VectorHit] = []
        for (n, _), item in self._items.items():
            if n != ns:
                continue
            if where and any(item.metadata.get(f) != v for f, v in where.items()):
                continue
            hits.append(VectorHit(item.id, cosine(vector, item.vector), item.metadata))
        hits.sort(key=lambda h: h.score, reverse=True)
        return hits[:k]

    async def delete(self, ns: str, ids: Sequence[str]) -> int:
        count = 0
        for id_ in ids:
            if self._items.pop((ns, id_), None) is not None:
                count += 1
        return count
