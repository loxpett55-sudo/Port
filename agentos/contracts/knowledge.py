"""Контракт системы знаний: курируемый, версионируемый корпус с
провенансом и уровнями доверия (в отличие от памяти — субъективного
опыта агентов)."""

from __future__ import annotations

import enum
import time
import uuid
from abc import ABC, abstractmethod
from dataclasses import dataclass, field
from typing import Any


class TrustLevel(enum.Enum):
    VERIFIED = "verified"
    INTERNAL = "internal"
    EXTERNAL = "external"
    UNVERIFIED = "unverified"


TRUST_WEIGHT = {
    TrustLevel.VERIFIED: 1.0,
    TrustLevel.INTERNAL: 0.9,
    TrustLevel.EXTERNAL: 0.7,
    TrustLevel.UNVERIFIED: 0.4,
}


@dataclass(frozen=True)
class Document:
    text: str
    source: str                          # uri/путь/идентификатор источника
    id: str = field(default_factory=lambda: uuid.uuid4().hex)
    title: str = ""
    trust: TrustLevel = TrustLevel.INTERNAL
    metadata: dict[str, Any] = field(default_factory=dict)
    time: float = field(default_factory=time.time)


@dataclass(frozen=True)
class KnowledgeHit:
    """Фрагмент знаний с полным провенансом — основа объяснимости."""

    text: str
    score: float
    document_id: str
    source: str
    trust: TrustLevel
    kb: str
    kb_version: int
    chunk_index: int


class KnowledgePort(ABC):
    """KnowledgePort@1."""

    @abstractmethod
    async def add_document(self, kb: str, document: Document) -> int:
        """Индексация документа; возвращает число созданных чанков."""

    @abstractmethod
    async def remove_document(self, kb: str, document_id: str) -> int: ...

    @abstractmethod
    async def retrieve(
        self, query: str, *, kbs: tuple[str, ...] = (), k: int = 8
    ) -> list[KnowledgeHit]:
        """Семантический поиск с ранжированием по релевантности × доверию."""

    @abstractmethod
    async def version(self, kb: str) -> int:
        """Текущая версия KB (инкремент при каждом изменении)."""
