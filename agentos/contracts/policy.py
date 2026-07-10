"""Контракт системы политик (PDP/PEP).

Политика — данные, а не код: правило (subject, action, resource,
condition) → effect с приоритетом и областью действия. Все Runtime
спрашивают Policy Runtime на границах (enforcement points).
"""

from __future__ import annotations

from abc import ABC, abstractmethod
from dataclasses import dataclass, field
from typing import Any, Callable

Matcher = dict[str, Any]        # подмножество атрибутов; значение "*" — любое
Condition = Callable[[dict[str, Any]], bool]


@dataclass(frozen=True)
class PolicyRule:
    id: str
    action: str                  # шаблон действия: "tool.invoke", "inference.*", "**"
    effect: str                  # allow | deny
    subject: Matcher = field(default_factory=dict)
    resource: Matcher = field(default_factory=dict)
    condition: Condition | None = None
    priority: int = 0            # больше — важнее
    scope: str = "global"        # global | tenant:<id> | session:<id> | agent:<id>
    obligations: tuple[str, ...] = ()   # предписания PEP: audit, mask-pii, confirm
    message: str = ""


@dataclass(frozen=True)
class Decision:
    allowed: bool
    reason: str = ""
    rule_id: str = ""
    obligations: tuple[str, ...] = ()


class PolicyPort(ABC):
    """PolicyPort@1."""

    @abstractmethod
    def add_rule(self, rule: PolicyRule) -> None: ...

    @abstractmethod
    def remove_rule(self, rule_id: str) -> bool: ...

    @abstractmethod
    def rules(self) -> list[PolicyRule]: ...

    @abstractmethod
    async def evaluate(
        self,
        *,
        action: str,
        subject: dict[str, Any] | None = None,
        resource: dict[str, Any] | None = None,
        context: dict[str, Any] | None = None,
    ) -> Decision:
        """Решение: правило с наибольшим приоритетом из совпавших;
        при равенстве deny побеждает; без совпадений — default effect."""
