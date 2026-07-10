"""Контракт событийной шины.

Вся платформа взаимодействует через события: типизированные,
версионированные, с трассировочными идентификаторами. Гарантия
доставки — at-least-once, обработчики обязаны быть идемпотентными.
"""

from __future__ import annotations

import enum
import time
import uuid
from abc import ABC, abstractmethod
from dataclasses import dataclass, field
from typing import Any, Awaitable, Callable


class Priority(enum.IntEnum):
    """Классы QoS. Меньше значение — выше приоритет."""

    INTERACTIVE = 0
    NORMAL = 1
    BACKGROUND = 2


@dataclass(frozen=True)
class Event:
    """Событие платформы. type — `domain.entity.action`, payload — по схеме."""

    type: str
    payload: dict[str, Any] = field(default_factory=dict)
    id: str = field(default_factory=lambda: uuid.uuid4().hex)
    version: int = 1
    time: float = field(default_factory=time.time)
    source: str = ""
    subject: str = ""
    correlation_id: str = ""
    causation_id: str = ""
    priority: Priority = Priority.NORMAL

    def caused(
        self, type: str, payload: dict[str, Any] | None = None, **kw: Any
    ) -> "Event":
        """Породить следствие: наследует корреляцию, фиксирует причину."""
        return Event(
            type=type,
            payload=payload or {},
            correlation_id=self.correlation_id or self.id,
            causation_id=self.id,
            priority=kw.pop("priority", self.priority),
            **kw,
        )


EventHandler = Callable[[Event], Awaitable[None]]


@dataclass(frozen=True)
class Subscription:
    id: str
    pattern: str


def topic_matches(pattern: str, topic: str) -> bool:
    """Сопоставление топика с шаблоном: `task.*` — один сегмент,
    `task.**` или `**` — любой хвост, `task.completed` — точное."""
    if pattern in ("**", "*"):
        return pattern == "**" or "." not in topic
    p_parts = pattern.split(".")
    t_parts = topic.split(".")
    for i, p in enumerate(p_parts):
        if p == "**":
            return True
        if i >= len(t_parts):
            return False
        if p != "*" and p != t_parts[i]:
            return False
    return len(p_parts) == len(t_parts)


class EventPort(ABC):
    """Порт шины событий (EventPort@1)."""

    @abstractmethod
    async def publish(self, event: Event) -> None:
        """Опубликовать событие немедленно."""

    @abstractmethod
    async def publish_at(self, event: Event, at_time: float) -> str:
        """Отложенная публикация (unix time). Возвращает id таймера."""

    @abstractmethod
    def subscribe(
        self, pattern: str, handler: EventHandler, *, durable: str | None = None
    ) -> Subscription:
        """Подписка по шаблону топика. durable — имя consumer group:
        подписчики с одним именем конкурируют за события."""

    @abstractmethod
    def unsubscribe(self, subscription: Subscription) -> None: ...

    @abstractmethod
    async def history(
        self, pattern: str = "**", *, since: float = 0.0, limit: int = 1000
    ) -> list[Event]:
        """История событий из Event Log (для replay и диагностики)."""

    @abstractmethod
    async def drain(self) -> None:
        """Дождаться доставки всех событий в полёте (тесты, shutdown)."""
