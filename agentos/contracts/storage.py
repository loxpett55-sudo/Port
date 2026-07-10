"""Порты хранения. Ни один Runtime не обращается к БД напрямую —
только через эти порты. Встроенные реализации: in-memory и SQLite."""

from __future__ import annotations

from abc import ABC, abstractmethod
from dataclasses import dataclass, field
from typing import Any, Sequence


class KVStorePort(ABC):
    """KVStorePort@1: ключ-значение с TTL и namespace-изоляцией."""

    @abstractmethod
    async def get(self, ns: str, key: str) -> Any | None: ...

    @abstractmethod
    async def put(self, ns: str, key: str, value: Any, *, ttl: float | None = None) -> None: ...

    @abstractmethod
    async def delete(self, ns: str, key: str) -> bool: ...

    @abstractmethod
    async def keys(self, ns: str, prefix: str = "") -> list[str]: ...


@dataclass(frozen=True)
class LogRecord:
    offset: int
    topic: str
    time: float
    data: dict[str, Any]


class EventLogPort(ABC):
    """EventLogPort@1: append-only журнал с чтением по offset."""

    @abstractmethod
    async def append(self, topic: str, data: dict[str, Any]) -> int: ...

    @abstractmethod
    async def read(
        self, *, since_offset: int = 0, since_time: float = 0.0, limit: int = 1000
    ) -> list[LogRecord]: ...


@dataclass
class VectorItem:
    id: str
    vector: Sequence[float]
    metadata: dict[str, Any] = field(default_factory=dict)


@dataclass(frozen=True)
class VectorHit:
    id: str
    score: float
    metadata: dict[str, Any]


class VectorStorePort(ABC):
    """VectorStorePort@1: векторный поиск с фильтрами по метаданным."""

    @abstractmethod
    async def upsert(self, ns: str, items: Sequence[VectorItem]) -> None: ...

    @abstractmethod
    async def search(
        self,
        ns: str,
        vector: Sequence[float],
        *,
        k: int = 10,
        where: dict[str, Any] | None = None,
    ) -> list[VectorHit]: ...

    @abstractmethod
    async def delete(self, ns: str, ids: Sequence[str]) -> int: ...
