"""Session Runtime — жизненный цикл сессий поверх KV (durable:
Suspended-сессия — запись в Storage, переживает рестарты)."""

from __future__ import annotations

from dataclasses import asdict
from typing import Any

from agentos.contracts.errors import AgentOSError
from agentos.contracts.events import Event, EventPort
from agentos.contracts.module import ModuleContext, ModuleManifest, RuntimeModule
from agentos.contracts.session import Session, SessionPort, SessionState, Turn
from agentos.contracts.storage import KVStorePort

_NS = "sessions"


class SessionService(SessionPort):
    def __init__(self, kv: KVStorePort, events: EventPort | None = None) -> None:
        self._kv = kv
        self._events = events

    async def open(self, principal: str, metadata: dict[str, Any] | None = None) -> Session:
        session = Session(principal=principal, metadata=metadata or {})
        await self._save(session)
        await self._emit("session.opened", session)
        return session

    async def get(self, session_id: str) -> Session:
        raw = await self._kv.get(_NS, session_id)
        if raw is None:
            raise AgentOSError(f"сессия не найдена: {session_id}")
        raw["state"] = SessionState(raw["state"])
        return Session(**raw)

    async def append_turn(self, session_id: str, turn: Turn) -> None:
        session = await self.get(session_id)
        if session.state is SessionState.CLOSED:
            raise AgentOSError(f"сессия закрыта: {session_id}")
        turns = await self._kv.get(_NS, f"{session_id}:turns") or []
        turns.append(asdict(turn))
        await self._kv.put(_NS, f"{session_id}:turns", turns)
        import time

        session.last_active = time.time()
        session.state = SessionState.ACTIVE
        await self._save(session)

    async def turns(self, session_id: str, limit: int = 50) -> list[Turn]:
        raw = await self._kv.get(_NS, f"{session_id}:turns") or []
        return [Turn(**t) for t in raw[-limit:]]

    async def suspend(self, session_id: str) -> None:
        await self._set_state(session_id, SessionState.SUSPENDED, "session.suspended")

    async def resume(self, session_id: str) -> Session:
        await self._set_state(session_id, SessionState.ACTIVE, "session.resumed")
        return await self.get(session_id)

    async def close(self, session_id: str) -> None:
        # session.closed запускает Memory ingestion итога диалога
        await self._set_state(session_id, SessionState.CLOSED, "session.closed")

    async def _set_state(self, session_id: str, state: SessionState, event: str) -> None:
        session = await self.get(session_id)
        session.state = state
        await self._save(session)
        await self._emit(event, session)

    async def _save(self, session: Session) -> None:
        data = asdict(session)
        data["state"] = session.state.value
        await self._kv.put(_NS, session.id, data)

    async def _emit(self, type_: str, session: Session) -> None:
        if self._events:
            await self._events.publish(
                Event(
                    type=type_,
                    payload={"principal": session.principal},
                    subject=f"session:{session.id}",
                )
            )


class SessionRuntime(RuntimeModule):
    def __init__(self) -> None:
        self._service: SessionService | None = None

    def manifest(self) -> ModuleManifest:
        return ModuleManifest(
            id="session-runtime",
            version="0.2.0",
            provides_ports=("SessionPort@1",),
            requires_ports=("KVStorePort@1", "EventPort@1"),
            provides_events=(
                "session.opened@1",
                "session.suspended@1",
                "session.resumed@1",
                "session.closed@1",
            ),
        )

    async def init(self, ctx: ModuleContext) -> None:
        self._service = SessionService(ctx.port("KVStorePort@1"), ctx.port("EventPort@1"))
        ctx.register("SessionPort@1", self._service)
