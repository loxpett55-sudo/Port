"""Контракт Context Runtime: автоматическая сборка контекста под бюджет."""

from __future__ import annotations

from abc import ABC, abstractmethod
from dataclasses import dataclass, field


def approx_tokens(text: str) -> int:
    """Единая оценка токенов платформы (4 символа ≈ 1 токен).
    Точные токенизаторы подключаются адаптерами."""
    return max(1, len(text) // 4) if text else 0


@dataclass(frozen=True)
class ContextRequest:
    intent: str                          # текущий запрос/цель потребителя
    session_id: str = ""
    agent_id: str = ""
    scope: str = ""                      # scope памяти (по умолчанию из сессии)
    budget_tokens: int = 4000
    include: tuple[str, ...] = ()        # явный выбор секций; пусто — решает Planner


@dataclass(frozen=True)
class ContextSection:
    name: str                            # system|conversation|memory|knowledge|tools|...
    content: str
    tokens: int
    provenance: tuple[str, ...] = ()     # источники фрагментов


@dataclass(frozen=True)
class ContextBundle:
    sections: tuple[ContextSection, ...]
    total_tokens: int
    budget_tokens: int

    def text(self) -> str:
        return "\n\n".join(
            f"## {s.name}\n{s.content}" for s in self.sections if s.content
        )


class ContextPort(ABC):
    """ContextPort@1."""

    @abstractmethod
    async def build(self, request: ContextRequest) -> ContextBundle: ...
