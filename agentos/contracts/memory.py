"""Контракт многоуровневой памяти."""

from __future__ import annotations

import enum
import time
import uuid
from abc import ABC, abstractmethod
from dataclasses import dataclass, field
from typing import Any


class MemoryLevel(enum.Enum):
    WORKING = "working"
    SHORT_TERM = "short_term"
    LONG_TERM = "long_term"
    SEMANTIC = "semantic"
    EPISODIC = "episodic"
    PROCEDURAL = "procedural"
    REFLECTION = "reflection"
    ARCHIVE = "archive"


@dataclass
class MemoryItem:
    text: str
    scope: str                           # user:<id> | agent:<id> | global
    level: MemoryLevel = MemoryLevel.SHORT_TERM
    id: str = field(default_factory=lambda: uuid.uuid4().hex)
    importance: float = 0.5              # 0..1
    created: float = field(default_factory=time.time)
    last_access: float = field(default_factory=time.time)
    access_count: int = 0
    metadata: dict[str, Any] = field(default_factory=dict)


@dataclass(frozen=True)
class MemoryHit:
    item: MemoryItem
    score: float


class MemoryPort(ABC):
    """MemoryPort@1."""

    @abstractmethod
    async def remember(self, item: MemoryItem) -> str: ...

    @abstractmethod
    async def recall(
        self,
        query: str,
        scope: str,
        *,
        levels: tuple[MemoryLevel, ...] = (),
        k: int = 8,
    ) -> list[MemoryHit]: ...

    @abstractmethod
    async def forget(self, scope: str, item_id: str) -> bool:
        """Право на забвение: удаляет запись и её вектор."""

    @abstractmethod
    async def consolidate(self, scope: str) -> int:
        """Short-Term → Long-Term по значимости; возвращает число повышений."""

    @abstractmethod
    async def archive(self, scope: str, *, max_idle_seconds: float) -> int:
        """Вытеснение невостребованного из Long-Term в Archive."""
