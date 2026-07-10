"""Контракт Workflow Engine: декларативный граф шагов с durable-состоянием.

Поддерживаемые конструкции: последовательность, ветвление, циклы,
условия, параллельные ветви, ожидание события, задержка, компенсации
(Saga). Шаг исполняется как задача Task Runtime либо встроенной
функцией; условия — вызываемые предикаты над переменными экземпляра.
"""

from __future__ import annotations

import enum
from abc import ABC, abstractmethod
from dataclasses import dataclass, field
from typing import Any, Awaitable, Callable

Vars = dict[str, Any]
StepFn = Callable[[Vars], Awaitable[Any]]
Predicate = Callable[[Vars], bool]


class StepKind(enum.Enum):
    ACTION = "action"          # функция или задача
    BRANCH = "branch"          # if/else
    LOOP = "loop"              # while cond (с лимитом итераций)
    PARALLEL = "parallel"      # ветви + join all
    WAIT_EVENT = "wait_event"  # ожидание события шины
    DELAY = "delay"            # ожидание времени


@dataclass(frozen=True)
class Step:
    id: str
    kind: StepKind = StepKind.ACTION
    # ACTION: run — функция; результат кладётся в vars[id]
    run: StepFn | None = None
    # ACTION: компенсация при откате Saga
    compensate: StepFn | None = None
    # BRANCH/LOOP: условие
    condition: Predicate | None = None
    # BRANCH: ветви then/else; LOOP: body; PARALLEL: branches
    then_steps: tuple["Step", ...] = ()
    else_steps: tuple["Step", ...] = ()
    branches: tuple[tuple["Step", ...], ...] = ()
    # LOOP
    max_iterations: int = 100
    # WAIT_EVENT
    event_pattern: str = ""
    timeout_seconds: float = 0.0           # 0 — без тайм-аута
    # DELAY
    delay_seconds: float = 0.0


@dataclass(frozen=True)
class WorkflowDef:
    id: str
    steps: tuple[Step, ...]
    version: str = "1.0.0"


class InstanceState(enum.Enum):
    RUNNING = "running"
    COMPLETED = "completed"
    FAILED = "failed"
    COMPENSATED = "compensated"


@dataclass
class WorkflowInstance:
    id: str
    workflow_id: str
    state: InstanceState = InstanceState.RUNNING
    vars: Vars = field(default_factory=dict)
    completed_steps: list[str] = field(default_factory=list)
    error: str = ""


class WorkflowPort(ABC):
    """WorkflowPort@1."""

    @abstractmethod
    def register(self, definition: WorkflowDef) -> None: ...

    @abstractmethod
    async def start(self, workflow_id: str, vars: Vars | None = None) -> str:
        """Запустить экземпляр; возвращает instance id."""

    @abstractmethod
    async def status(self, instance_id: str) -> WorkflowInstance: ...

    @abstractmethod
    async def wait(self, instance_id: str, timeout: float = 60.0) -> WorkflowInstance:
        """Дождаться завершения экземпляра."""
