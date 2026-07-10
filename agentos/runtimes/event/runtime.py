"""Event Runtime — шина событий платформы.

Возможности: публикация/подписка по шаблонам топиков, приоритеты QoS,
consumer groups (durable-подписки), отложенные события, история из
Event Log, drain для graceful shutdown. Доставка at-least-once:
обработчики обязаны быть идемпотентными.
"""

from __future__ import annotations

import asyncio
import heapq
import itertools
import logging
import time
import uuid
from dataclasses import asdict

from agentos.contracts.events import (
    Event,
    EventHandler,
    EventPort,
    Subscription,
    topic_matches,
)
from agentos.contracts.module import ModuleContext, ModuleManifest, RuntimeModule
from agentos.contracts.storage import EventLogPort


class _Sub:
    def __init__(self, sub: Subscription, handler: EventHandler, durable: str | None):
        self.sub = sub
        self.handler = handler
        self.durable = durable


class EventBus(EventPort):
    def __init__(self, event_log: EventLogPort, source_default: str = "") -> None:
        self._log = event_log
        self._subs: dict[str, _Sub] = {}
        self._rr: dict[str, itertools.cycle] = {}
        self._queue: list[tuple[int, int, Event]] = []  # (priority, seq, event)
        self._seq = itertools.count()
        self._delayed: list[tuple[float, int, Event]] = []
        self._wakeup = asyncio.Event()
        self._inflight = 0
        self._idle = asyncio.Event()
        self._idle.set()
        self._worker: asyncio.Task | None = None
        self._timer: asyncio.Task | None = None
        self._running = False
        self._logger = logging.getLogger("agentos.event-runtime")
        self._source_default = source_default

    # --- жизненный цикл -------------------------------------------------

    async def start(self) -> None:
        self._running = True
        self._worker = asyncio.create_task(self._dispatch_loop())
        self._timer = asyncio.create_task(self._timer_loop())

    async def stop(self) -> None:
        await self.drain()
        self._running = False
        self._wakeup.set()
        for task in (self._worker, self._timer):
            if task:
                task.cancel()
                try:
                    await task
                except asyncio.CancelledError:
                    pass

    # --- EventPort --------------------------------------------------------

    async def publish(self, event: Event) -> None:
        await self._log.append(event.type, asdict(event))
        self._enqueue(event)

    async def publish_at(self, event: Event, at_time: float) -> str:
        timer_id = uuid.uuid4().hex
        heapq.heappush(self._delayed, (at_time, next(self._seq), event))
        self._wakeup.set()
        return timer_id

    def subscribe(
        self, pattern: str, handler: EventHandler, *, durable: str | None = None
    ) -> Subscription:
        sub = Subscription(id=uuid.uuid4().hex, pattern=pattern)
        self._subs[sub.id] = _Sub(sub, handler, durable)
        self._rr.pop(durable or "", None)  # состав группы изменился
        return sub

    def unsubscribe(self, subscription: Subscription) -> None:
        removed = self._subs.pop(subscription.id, None)
        if removed and removed.durable:
            self._rr.pop(removed.durable, None)

    async def history(
        self, pattern: str = "**", *, since: float = 0.0, limit: int = 1000
    ) -> list[Event]:
        records = await self._log.read(since_time=since, limit=limit * 4)
        out = []
        for r in records:
            if topic_matches(pattern, r.topic):
                data = dict(r.data)
                data["priority"] = data.get("priority", 1)
                out.append(Event(**data))
                if len(out) >= limit:
                    break
        return out

    async def drain(self) -> None:
        while self._queue or self._inflight:
            await self._idle.wait()
            await asyncio.sleep(0)  # дать воркеру взять следующее из очереди

    # --- внутреннее -------------------------------------------------------

    def _enqueue(self, event: Event) -> None:
        heapq.heappush(self._queue, (int(event.priority), next(self._seq), event))
        self._idle.clear()
        self._wakeup.set()

    def _targets(self, event: Event) -> list[_Sub]:
        matched = [s for s in self._subs.values() if topic_matches(s.sub.pattern, event.type)]
        # consumer groups: из группы событие получает один подписчик (round-robin)
        groups: dict[str, list[_Sub]] = {}
        singles: list[_Sub] = []
        for s in matched:
            if s.durable:
                groups.setdefault(s.durable, []).append(s)
            else:
                singles.append(s)
        for name, members in groups.items():
            if name not in self._rr:
                self._rr[name] = itertools.cycle(members)
            chosen = next(self._rr[name])
            # cycle мог быть построен по старому составу
            singles.append(chosen if chosen in members else members[0])
        return singles

    async def _dispatch_loop(self) -> None:
        while self._running:
            if not self._queue:
                self._idle.set()
                self._wakeup.clear()
                await self._wakeup.wait()
                continue
            _, _, event = heapq.heappop(self._queue)
            self._inflight += 1
            self._idle.clear()
            try:
                for target in self._targets(event):
                    try:
                        await target.handler(event)
                    except Exception:
                        # сбой одного подписчика не роняет шину и не блокирует остальных
                        self._logger.exception(
                            "обработчик %s: ошибка на событии %s",
                            target.sub.pattern,
                            event.type,
                        )
            finally:
                self._inflight -= 1
                if not self._queue and not self._inflight:
                    self._idle.set()

    async def _timer_loop(self) -> None:
        while self._running:
            now = time.time()
            while self._delayed and self._delayed[0][0] <= now:
                _, _, event = heapq.heappop(self._delayed)
                await self._log.append(event.type, asdict(event))
                self._enqueue(event)
            delay = (self._delayed[0][0] - now) if self._delayed else 0.5
            await asyncio.sleep(min(max(delay, 0.005), 0.5))


class EventRuntime(RuntimeModule):
    """Модуль-обёртка EventBus для ядра. Требует EventLogPort от Storage."""

    def __init__(self) -> None:
        self._bus: EventBus | None = None

    def manifest(self) -> ModuleManifest:
        return ModuleManifest(
            id="event-runtime",
            version="0.2.0",
            provides_ports=("EventPort@1",),
            requires_ports=("EventLogPort@1",),
            permissions=("storage:eventlog:read-write",),
        )

    async def init(self, ctx: ModuleContext) -> None:
        event_log: EventLogPort = ctx.port("EventLogPort@1")
        self._bus = EventBus(event_log)
        ctx.register("EventPort@1", self._bus)

    async def start(self) -> None:
        assert self._bus is not None
        await self._bus.start()

    async def stop(self, deadline_seconds: float = 10.0) -> None:
        if self._bus:
            await asyncio.wait_for(self._bus.stop(), timeout=deadline_seconds)

    @property
    def bus(self) -> EventBus:
        assert self._bus is not None, "EventRuntime не инициализирован"
        return self._bus
