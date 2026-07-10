"""Контракт Resource Runtime: бюджеты и учёт ресурсов.

Контекстное окно и токены/стоимость — ресурсы первого класса: они
исчерпываются раньше CPU. Учёт потребления — пассивный (события
inference.completed), enforcement — активный (check перед действием).
"""

from __future__ import annotations

from abc import ABC, abstractmethod
from dataclasses import dataclass, field


@dataclass
class Budget:
    """Лимиты на scope (session:/agent:/tenant:/global) за период."""

    max_tokens: int = 0            # 0 — без лимита
    max_cost: float = 0.0
    max_tool_calls: int = 0


@dataclass
class ResourceUsage:
    tokens: int = 0
    cost: float = 0.0
    tool_calls: int = 0


class ResourcePort(ABC):
    """ResourcePort@1."""

    @abstractmethod
    def set_budget(self, scope: str, budget: Budget) -> None: ...

    @abstractmethod
    def usage(self, scope: str) -> ResourceUsage: ...

    @abstractmethod
    def check(self, scope: str) -> None:
        """Поднимает ResourceExhaustedError, если бюджет scope исчерпан."""

    @abstractmethod
    def consume(self, scope: str, *, tokens: int = 0, cost: float = 0.0, tool_calls: int = 0) -> None: ...

    @abstractmethod
    def context_budget(self, scope: str, requested: int) -> int:
        """Выдать бюджет контекста: min(запрошенное, остаток токенов)."""
