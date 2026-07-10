"""Контракт моделей: единый интерфейс к любому провайдеру.

Добавление модели не меняет архитектуру: новый адаптер реализует
ModelPort и регистрируется в реестре. Потребители выбирают модель по
требованиям (capabilities), а не по имени.
"""

from __future__ import annotations

from abc import ABC, abstractmethod
from dataclasses import dataclass, field
from typing import Any, AsyncIterator, Sequence


# Возможности моделей (открытый набор строк; базовые — константы)
CAP_TEXT = "text"
CAP_VISION = "vision"
CAP_AUDIO = "audio"
CAP_TOOLS = "tools"
CAP_JSON = "json-mode"
CAP_EMBED = "embed"


@dataclass(frozen=True)
class ModelDescriptor:
    id: str
    provider: str
    capabilities: frozenset[str] = frozenset({CAP_TEXT})
    context_window: int = 8192
    max_output: int = 4096
    cost_in_per_1k: float = 0.0   # у.е. за 1k токенов входа
    cost_out_per_1k: float = 0.0
    locality: str = "local"       # local | cloud
    modalities: tuple[str, ...] = ("text",)


@dataclass(frozen=True)
class ChatMessage:
    role: str                     # system | user | assistant | tool
    content: str
    tool_call_id: str = ""
    name: str = ""


@dataclass(frozen=True)
class ToolCall:
    id: str
    name: str
    arguments: dict[str, Any]


@dataclass(frozen=True)
class ToolSchema:
    """Описание инструмента для модели (подмножество манифеста)."""

    name: str
    description: str
    input_schema: dict[str, Any]


@dataclass(frozen=True)
class GenerateRequest:
    messages: tuple[ChatMessage, ...]
    tools: tuple[ToolSchema, ...] = ()
    max_tokens: int = 1024
    temperature: float = 0.7
    json_mode: bool = False
    stop: tuple[str, ...] = ()


@dataclass(frozen=True)
class Usage:
    prompt_tokens: int = 0
    completion_tokens: int = 0
    cost: float = 0.0


@dataclass(frozen=True)
class GenerateChunk:
    """Единица стриминга: дельта текста, вызов инструмента или финал."""

    delta: str = ""
    tool_call: ToolCall | None = None
    finish_reason: str = ""       # "" | stop | tool_use | length | error
    usage: Usage | None = None    # присутствует в финальном чанке


@dataclass(frozen=True)
class GenerateResult:
    text: str
    tool_calls: tuple[ToolCall, ...] = ()
    finish_reason: str = "stop"
    usage: Usage = field(default_factory=Usage)


class ModelPort(ABC):
    """ModelPort@1 — контракт адаптера модели."""

    @abstractmethod
    def descriptor(self) -> ModelDescriptor: ...

    @abstractmethod
    def generate(self, request: GenerateRequest) -> AsyncIterator[GenerateChunk]:
        """Потоковая генерация. Нестриминговые провайдеры отдают один чанк."""

    async def embed(self, texts: Sequence[str]) -> list[list[float]]:
        raise NotImplementedError(f"{self.descriptor().id}: embed не поддерживается")


@dataclass(frozen=True)
class ModelRequirements:
    """Запрос модели по требованиям, а не по имени."""

    capabilities: frozenset[str] = frozenset({CAP_TEXT})
    min_context: int = 0
    locality: str = ""            # "" — любая; local | cloud
    prefer: str = "cheapest"      # cheapest | largest-context
    model_id: str = ""            # явный выбор (обходит подбор)


class ModelRegistryPort(ABC):
    """ModelRegistryPort@1 — реестр моделей."""

    @abstractmethod
    def register(self, adapter: ModelPort, *, aliases: Sequence[str] = ()) -> None: ...

    @abstractmethod
    def select(self, requirements: ModelRequirements) -> ModelPort: ...

    @abstractmethod
    def get(self, model_id: str) -> ModelPort: ...

    @abstractmethod
    def list(self) -> list[ModelDescriptor]: ...
