"""DistributedAgentGateway — прозрачная маршрутизация сообщений агентам
по кластеру.

Владелец экземпляра агента определяется consistent hashing по ключу
`agent_id/session_id`. Локальный владелец — прямой вызов; удалённый —
request/reply через федеративную шину (cluster.agent.request /
cluster.agent.reply с корреляцией по id запроса).
"""

from __future__ import annotations

import asyncio
import uuid

from agentos.contracts.agent import AgentPort, AgentReply
from agentos.contracts.errors import AgentOSError
from agentos.contracts.events import Event, EventPort
from agentos.cluster.hashring import ConsistentHashRing


class DistributedAgentGateway:
    def __init__(
        self,
        bus: EventPort,
        ring: ConsistentHashRing,
        node_id: str,
        local_agents: AgentPort | None = None,
    ) -> None:
        self._bus = bus
        self._ring = ring
        self.node_id = node_id
        self._local = local_agents
        self._pending: dict[str, asyncio.Future] = {}

    def start(self) -> None:
        self._bus.subscribe("cluster.agent.request", self._on_request)
        self._bus.subscribe("cluster.agent.reply", self._on_reply)

    # --- публичное API ---------------------------------------------------------

    async def send(
        self, agent_id: str, content: str, *, session_id: str = "", timeout: float = 60.0
    ) -> AgentReply:
        owner = self._ring.owner(f"{agent_id}/{session_id}")
        if owner == self.node_id:
            if self._local is None:
                raise AgentOSError(f"узел {self.node_id}: нет локального AgentPort")
            return await self._local.send(agent_id, content, session_id=session_id)

        request_id = uuid.uuid4().hex
        future: asyncio.Future = asyncio.get_running_loop().create_future()
        self._pending[request_id] = future
        await self._bus.publish(
            Event(
                type="cluster.agent.request",
                payload={
                    "request_id": request_id,
                    "target_node": owner,
                    "agent_id": agent_id,
                    "content": content,
                    "session_id": session_id,
                },
                subject=f"agent:{agent_id}",
            )
        )
        try:
            return await asyncio.wait_for(future, timeout)
        finally:
            self._pending.pop(request_id, None)

    # --- обработчики шины ---------------------------------------------------------

    async def _on_request(self, event: Event) -> None:
        if event.payload.get("target_node") != self.node_id or self._local is None:
            return
        request_id = event.payload["request_id"]
        try:
            reply = await self._local.send(
                event.payload["agent_id"],
                event.payload["content"],
                session_id=event.payload.get("session_id", ""),
            )
            payload = {
                "request_id": request_id,
                "ok": True,
                "text": reply.text,
                "iterations": reply.iterations,
                "tool_calls": reply.tool_calls,
            }
        except Exception as e:
            payload = {"request_id": request_id, "ok": False, "error": str(e)}
        await self._bus.publish(event.caused("cluster.agent.reply", payload))

    async def _on_reply(self, event: Event) -> None:
        future = self._pending.get(event.payload.get("request_id", ""))
        if future is None or future.done():
            return
        if event.payload.get("ok"):
            future.set_result(
                AgentReply(
                    text=event.payload["text"],
                    iterations=event.payload["iterations"],
                    tool_calls=event.payload["tool_calls"],
                )
            )
        else:
            future.set_exception(AgentOSError(event.payload.get("error", "удалённый сбой")))
