"""Контракт планировщика: таймеры, периодические задачи, DAG зависимостей."""

from __future__ import annotations

from abc import ABC, abstractmethod
from dataclasses import dataclass, field
from typing import Any

from agentos.contracts.task import TaskSpec


@dataclass(frozen=True)
class DagSpec:
    """Граф задач: задача стартует после успеха всех зависимостей."""

    tasks: dict[str, TaskSpec]              # имя узла → задача
    dependencies: dict[str, tuple[str, ...]] = field(default_factory=dict)


@dataclass
class DagStatus:
    dag_id: str
    done: bool
    node_states: dict[str, str] = field(default_factory=dict)
    results: dict[str, Any] = field(default_factory=dict)
    failed: bool = False


class SchedulerPort(ABC):
    """SchedulerPort@1."""

    @abstractmethod
    async def at(self, when: float, spec: TaskSpec) -> str:
        """Одноразовый таймер (unix time) → задача. Возвращает timer id."""

    @abstractmethod
    async def every(self, interval_seconds: float, spec: TaskSpec) -> str:
        """Периодическая задача. Возвращает timer id."""

    @abstractmethod
    async def cancel_timer(self, timer_id: str) -> bool: ...

    @abstractmethod
    async def submit_dag(self, dag: DagSpec) -> str: ...

    @abstractmethod
    async def dag_status(self, dag_id: str) -> DagStatus: ...
