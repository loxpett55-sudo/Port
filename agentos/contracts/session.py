"""Контракт сессий: устойчивые границы взаимодействия с платформой."""

from __future__ import annotations

import enum
import time
import uuid
from abc import ABC, abstractmethod
from dataclasses import dataclass, field
from typing import Any


class SessionState(enum.Enum):
    ACTIVE = "active"
    SUSPENDED = "suspended"
    CLOSED = "closed"


@dataclass
class Session:
    id: str = field(default_factory=lambda: uuid.uuid4().hex)
    principal: str = ""                 # идентичность пользователя
    state: SessionState = SessionState.ACTIVE
    agents: list[str] = field(default_factory=list)
    created: float = field(default_factory=time.time)
    last_active: float = field(default_factory=time.time)
    metadata: dict[str, Any] = field(default_factory=dict)


@dataclass(frozen=True)
class Turn:
    """Ход диалога (Conversation Context)."""

    role: str                           # user | assistant | system | tool
    content: str
    time: float = field(default_factory=time.time)
    agent_id: str = ""


class SessionPort(ABC):
    """SessionPort@1."""

    @abstractmethod
    async def open(self, principal: str, metadata: dict[str, Any] | None = None) -> Session: ...

    @abstractmethod
    async def get(self, session_id: str) -> Session: ...

    @abstractmethod
    async def append_turn(self, session_id: str, turn: Turn) -> None: ...

    @abstractmethod
    async def turns(self, session_id: str, limit: int = 50) -> list[Turn]: ...

    @abstractmethod
    async def suspend(self, session_id: str) -> None: ...

    @abstractmethod
    async def resume(self, session_id: str) -> Session: ...

    @abstractmethod
    async def close(self, session_id: str) -> None: ...
