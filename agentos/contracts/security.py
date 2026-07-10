"""Контракты Security Runtime: permissions, секреты, аудит."""

from __future__ import annotations

import time
from abc import ABC, abstractmethod
from dataclasses import dataclass, field
from typing import Any


def capability_matches(grant: str, required: str) -> bool:
    """Иерархические capabilities: grant 'storage:*' покрывает
    'storage:kv:read'; '*' покрывает всё."""
    if grant == "*":
        return True
    g_parts = grant.split(":")
    r_parts = required.split(":")
    for i, g in enumerate(g_parts):
        if g == "*":
            return True
        if i >= len(r_parts) or g != r_parts[i]:
            return False
    return len(g_parts) == len(r_parts)


@dataclass(frozen=True)
class PermissionSet:
    grants: frozenset[str] = frozenset()

    def allows(self, required: str) -> bool:
        return any(capability_matches(g, required) for g in self.grants)


@dataclass(frozen=True)
class SecretLease:
    """Короткоживущая выдача секрета. Значение не должно попадать в
    контекст моделей, логи и события."""

    name: str
    value: str
    expires: float

    @property
    def alive(self) -> bool:
        return time.time() < self.expires


@dataclass(frozen=True)
class AuditRecord:
    index: int
    time: float
    actor: str
    action: str
    detail: dict[str, Any]
    prev_hash: str
    hash: str


class SecretsPort(ABC):
    """SecretsPort@1."""

    @abstractmethod
    async def put(self, name: str, value: str) -> None: ...

    @abstractmethod
    async def lease(self, name: str, *, ttl: float = 60.0, actor: str = "") -> SecretLease: ...

    @abstractmethod
    async def delete(self, name: str) -> bool: ...


class AuditPort(ABC):
    """AuditPort@1 — append-only журнал с криптографическим сцеплением."""

    @abstractmethod
    async def record(self, actor: str, action: str, detail: dict[str, Any]) -> AuditRecord: ...

    @abstractmethod
    async def read(self, *, since_index: int = 0, limit: int = 1000) -> list[AuditRecord]: ...

    @abstractmethod
    async def verify(self) -> bool:
        """Проверка целостности цепочки хэшей."""
