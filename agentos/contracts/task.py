"""Контракт задач: атомарная единица работы с состоянием и повторами."""

from __future__ import annotations

import enum
import time
import uuid
from abc import ABC, abstractmethod
from dataclasses import dataclass, field
from typing import Any, Awaitable, Callable

from agentos.contracts.events import Priority


class TaskState(enum.Enum):
    PENDING = "pending"
    RUNNING = "running"
    SUCCEEDED = "succeeded"
    FAILED = "failed"
    RETRYING = "retrying"
    CANCELLED = "cancelled"
    DEAD_LETTERED = "dead_lettered"


@dataclass(frozen=True)
class TaskSpec:
    type: str                               # тип задачи → зарегистрированный обработчик
    input: dict[str, Any] = field(default_factory=dict)
    id: str = field(default_factory=lambda: uuid.uuid4().hex)
    priority: Priority = Priority.NORMAL
    max_attempts: int = 3
    timeout_seconds: float = 60.0
    idempotency_key: str = ""


@dataclass
class TaskRecord:
    spec: TaskSpec
    state: TaskState = TaskState.PENDING
    attempts: int = 0
    result: Any = None
    error: str = ""
    created: float = field(default_factory=time.time)
    updated: float = field(default_factory=time.time)


TaskHandler = Callable[[dict[str, Any]], Awaitable[Any]]


class TaskPort(ABC):
    """TaskPort@1."""

    @abstractmethod
    def register_handler(self, task_type: str, handler: TaskHandler) -> None: ...

    @abstractmethod
    async def submit(self, spec: TaskSpec) -> str:
        """Поставить задачу; идемпотентно по idempotency_key."""

    @abstractmethod
    async def get(self, task_id: str) -> TaskRecord: ...

    @abstractmethod
    async def cancel(self, task_id: str) -> bool: ...

    @abstractmethod
    async def result(self, task_id: str, timeout: float = 60.0) -> Any:
        """Дождаться завершения и вернуть результат (или поднять ошибку)."""
