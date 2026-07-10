"""Контракт Inference Runtime: надёжное исполнение запросов к моделям.

Model Runtime отвечает «что вызывать», Inference — «как исполнять»:
очереди, приоритеты, ретраи, кэш, стриминг, учёт токенов.
"""

from __future__ import annotations

from abc import ABC, abstractmethod
from dataclasses import dataclass, field
from typing import AsyncIterator

from agentos.contracts.events import Priority
from agentos.contracts.model import (
    GenerateChunk,
    GenerateRequest,
    GenerateResult,
    ModelRequirements,
)


@dataclass(frozen=True)
class InferenceRequest:
    request: GenerateRequest
    requirements: ModelRequirements = field(default_factory=ModelRequirements)
    priority: Priority = Priority.NORMAL
    session_id: str = ""
    agent_id: str = ""
    use_cache: bool = True
    max_attempts: int = 3


class InferencePort(ABC):
    """InferencePort@1."""

    @abstractmethod
    async def generate(self, req: InferenceRequest) -> GenerateResult:
        """Полный ответ (внутри может стримиться и собираться)."""

    @abstractmethod
    def stream(self, req: InferenceRequest) -> AsyncIterator[GenerateChunk]:
        """Потоковый ответ (кэш для стриминга не используется)."""

    @abstractmethod
    async def embed(self, texts: list[str], requirements: ModelRequirements) -> list[list[float]]:
        """Эмбеддинги через модель с capability `embed` (с батчингом)."""
