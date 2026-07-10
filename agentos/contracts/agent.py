"""Контракт агентов: агент — композиция независимых компонентов,
исполняемая как актор (почтовый ящик, последовательная обработка,
изолированное состояние, supervision)."""

from __future__ import annotations

from abc import ABC, abstractmethod
from dataclasses import dataclass, field

from agentos.contracts.model import ModelRequirements


@dataclass(frozen=True)
class AgentDefinition:
    """Декларативная композиция агента. Каждый компонент заменяем:
    personality/goals — данные; reasoner/planner — стратегии из реестра."""

    id: str
    personality: str = "Полезный ассистент."
    goals: tuple[str, ...] = ()
    tools: tuple[str, ...] = ()             # allow-list инструментов
    knowledge_bases: tuple[str, ...] = ()
    memory_scope: str = ""                  # по умолчанию agent:<id>
    model: ModelRequirements = field(default_factory=ModelRequirements)
    reasoner: str = "tool-loop"             # стратегия из реестра reasoner-ов
    max_iterations: int = 8                 # лимит автономии на ход
    context_budget: int = 4000


@dataclass(frozen=True)
class AgentReply:
    text: str
    iterations: int
    tool_calls: int


class AgentPort(ABC):
    """AgentPort@1."""

    @abstractmethod
    def register(self, definition: AgentDefinition) -> None: ...

    @abstractmethod
    def definitions(self) -> list[AgentDefinition]: ...

    @abstractmethod
    async def send(
        self, agent_id: str, content: str, *, session_id: str = ""
    ) -> AgentReply:
        """Сообщение агенту; обрабатывается его актором последовательно."""

    @abstractmethod
    async def stop_instance(self, agent_id: str, session_id: str = "") -> None:
        """Остановить актор (kill-switch)."""
