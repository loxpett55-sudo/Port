"""Контракт инструментов: инструменты — управляемые сервисы с манифестом,
правами, политикой исполнения и метриками."""

from __future__ import annotations

from abc import ABC, abstractmethod
from dataclasses import dataclass, field
from typing import Any, Awaitable, Callable


@dataclass(frozen=True)
class ToolManifest:
    id: str
    version: str
    description: str
    input_schema: dict[str, Any]
    output_schema: dict[str, Any] = field(default_factory=dict)
    permissions: tuple[str, ...] = ()      # что нужно инструменту
    timeout_seconds: float = 30.0
    sandbox: str = "none"                  # none | thread | process | container
    data_classes: tuple[str, ...] = ("public",)
    dependencies: tuple[str, ...] = ()
    stateful: str = "stateless"            # stateless | session | persistent


@dataclass(frozen=True)
class ToolCallContext:
    """Кто и от чьего имени вызывает инструмент (для политик и аудита)."""

    session_id: str = ""
    agent_id: str = ""
    principal: str = ""                    # идентичность пользователя
    correlation_id: str = ""


@dataclass(frozen=True)
class ToolResult:
    ok: bool
    output: Any = None
    error: str = ""
    duration_seconds: float = 0.0


ToolHandler = Callable[[dict[str, Any], ToolCallContext], Awaitable[Any]]


class ToolPort(ABC):
    """ToolPort@1 — реестр и шлюз вызова инструментов."""

    @abstractmethod
    def register(self, manifest: ToolManifest, handler: ToolHandler) -> None: ...

    @abstractmethod
    def unregister(self, tool_id: str) -> None: ...

    @abstractmethod
    def list(self) -> list[ToolManifest]: ...

    @abstractmethod
    def get(self, tool_id: str) -> ToolManifest: ...

    @abstractmethod
    async def invoke(
        self, tool_id: str, arguments: dict[str, Any], context: ToolCallContext
    ) -> ToolResult:
        """Вызов: политика → валидация входа → sandbox/timeout →
        валидация выхода → аудит и метрики."""
