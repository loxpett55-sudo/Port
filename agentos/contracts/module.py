"""Контракт Runtime-модуля: манифест, жизненный цикл, контекст.

Каждая подсистема платформы (Event, Memory, Model, ...) реализует
RuntimeModule и описывает себя манифестом. Ядро ничего не знает о
содержимом модулей — только об их контрактах.
"""

from __future__ import annotations

import enum
import logging
from abc import ABC, abstractmethod
from dataclasses import dataclass, field
from typing import Any, TYPE_CHECKING

if TYPE_CHECKING:  # только для типов; циклических зависимостей нет
    from agentos.contracts.events import Event, EventPort


class HealthState(enum.Enum):
    OK = "ok"
    DEGRADED = "degraded"
    FAILED = "failed"


@dataclass(frozen=True)
class HealthStatus:
    state: HealthState = HealthState.OK
    detail: str = ""

    @classmethod
    def ok(cls) -> "HealthStatus":
        return cls(HealthState.OK)


class ModuleState(enum.Enum):
    DISCOVERED = "discovered"
    RESOLVED = "resolved"
    INITIALIZED = "initialized"
    RUNNING = "running"
    DEGRADED = "degraded"
    STOPPING = "stopping"
    STOPPED = "stopped"
    FAILED = "failed"


@dataclass(frozen=True)
class PortRef:
    """Ссылка на порт: имя + major-версия (Interface First, semver)."""

    name: str
    major: int = 1

    def key(self) -> str:
        return f"{self.name}@{self.major}"

    @classmethod
    def parse(cls, spec: str) -> "PortRef":
        if "@" in spec:
            name, _, ver = spec.partition("@")
            return cls(name, int(ver))
        return cls(spec)


@dataclass(frozen=True)
class ModuleManifest:
    """Декларация модуля: что предоставляет, что требует, что ему можно."""

    id: str
    version: str
    provides_ports: tuple[str, ...] = ()
    requires_ports: tuple[str, ...] = ()
    # мягкие зависимости: учитываются в порядке запуска, если провайдер
    # присутствует в сборке, но их отсутствие не является ошибкой
    optional_ports: tuple[str, ...] = ()
    provides_events: tuple[str, ...] = ()
    consumes_events: tuple[str, ...] = ()
    permissions: tuple[str, ...] = ()
    config_schema: dict[str, Any] = field(default_factory=dict)


class ModuleContext:
    """Всё, что модуль получает от ядра: DI, шина, конфигурация, логгер.

    Модули не тянут глобальное состояние — только этот контекст.
    """

    def __init__(
        self,
        module_id: str,
        resolve: Any,
        register: Any,
        config: dict[str, Any],
        event_port: "EventPort | None" = None,
    ) -> None:
        self.module_id = module_id
        self._resolve = resolve
        self._register = register
        self.config = config
        self._event_port = event_port
        self.logger = logging.getLogger(f"agentos.{module_id}")

    def port(self, spec: str) -> Any:
        """Разрешить требуемый порт через Service Registry ядра."""
        return self._resolve(PortRef.parse(spec))

    def register(self, spec: str, provider: Any) -> None:
        """Зарегистрировать предоставляемый порт (владелец — этот модуль)."""
        self._register(spec, provider, self.module_id)

    @property
    def events(self) -> "EventPort":
        if self._event_port is None:
            self._event_port = self.port("EventPort@1")
        return self._event_port


class RuntimeModule(ABC):
    """Базовый контракт всех Runtime-модулей платформы."""

    @abstractmethod
    def manifest(self) -> ModuleManifest: ...

    @abstractmethod
    async def init(self, ctx: ModuleContext) -> None:
        """Регистрация портов и подписок. Ещё не обрабатывает трафик."""

    async def start(self) -> None:
        """Начало обработки. По умолчанию — no-op."""

    async def stop(self, deadline_seconds: float = 10.0) -> None:
        """Graceful shutdown с дедлайном. По умолчанию — no-op."""

    def health(self) -> HealthStatus:
        return HealthStatus.ok()
