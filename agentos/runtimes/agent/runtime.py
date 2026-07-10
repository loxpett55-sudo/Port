"""Agent Runtime — исполнение агентов как акторов.

Каждый экземпляр агента (agent_id × session_id) — актор: почтовый ящик,
последовательная обработка, изолированное состояние, supervision
(перезапуск актора при сбое). Ход агента: сборка контекста → reasoner-
цикл (инференс → инструменты → наблюдения) → ingest в память.

Стратегии рассуждения (reasoner) — заменяемые компоненты; встроенная —
`tool-loop` (ReAct-подобный цикл вызова инструментов).
"""

from __future__ import annotations

import asyncio
import logging
from dataclasses import dataclass, field
from typing import Any, Awaitable, Callable

from agentos.contracts.agent import AgentDefinition, AgentPort, AgentReply
from agentos.contracts.context import ContextPort, ContextRequest
from agentos.contracts.errors import AgentOSError
from agentos.contracts.events import Event, EventPort
from agentos.contracts.inference import InferencePort, InferenceRequest
from agentos.contracts.memory import MemoryItem, MemoryPort
from agentos.contracts.model import ChatMessage, GenerateRequest, ToolSchema
from agentos.contracts.module import ModuleContext, ModuleManifest, RuntimeModule
from agentos.contracts.session import SessionPort, Turn
from agentos.contracts.tool import ToolCallContext, ToolPort

log = logging.getLogger("agentos.agent-runtime")

# Reasoner: стратегия хода. Получает подготовленные сообщения и сервисы,
# возвращает финальный текст. Регистрируются по имени (плагины).
ReasonerFn = Callable[["_TurnEnv"], Awaitable[AgentReply]]


@dataclass
class _TurnEnv:
    definition: AgentDefinition
    messages: list[ChatMessage]
    tools: tuple[ToolSchema, ...]
    inference: InferencePort
    tool_port: ToolPort | None
    session_id: str


async def tool_loop_reasoner(env: _TurnEnv) -> AgentReply:
    """Встроенная стратегия: цикл инференс → инструменты → наблюдения,
    до финального текста или лимита итераций (границы автономии)."""
    d = env.definition
    tool_calls_total = 0
    for iteration in range(1, d.max_iterations + 1):
        result = await env.inference.generate(
            InferenceRequest(
                request=GenerateRequest(
                    messages=tuple(env.messages), tools=env.tools
                ),
                requirements=d.model,
                session_id=env.session_id,
                agent_id=d.id,
            )
        )
        if not result.tool_calls:
            return AgentReply(
                text=result.text, iterations=iteration, tool_calls=tool_calls_total
            )
        if env.tool_port is None:
            raise AgentOSError(f"агент {d.id}: модель требует инструменты, ToolPort нет")
        for call in result.tool_calls:
            if d.tools and call.name not in d.tools:
                observation = f"инструмент {call.name} не разрешён этому агенту"
            else:
                outcome = await env.tool_port.invoke(
                    call.name,
                    call.arguments,
                    ToolCallContext(session_id=env.session_id, agent_id=d.id),
                )
                observation = (
                    str(outcome.output) if outcome.ok else f"ошибка: {outcome.error}"
                )
            tool_calls_total += 1
            env.messages.append(
                ChatMessage(role="assistant", content=f"[вызов {call.name}]")
            )
            env.messages.append(
                ChatMessage(role="tool", content=observation, tool_call_id=call.id, name=call.name)
            )
    raise AgentOSError(
        f"агент {d.id}: лимит итераций {d.max_iterations} исчерпан без ответа"
    )


class _Actor:
    """Актор экземпляра агента: mailbox + последовательный цикл + supervision."""

    def __init__(self, process: Callable[[str], Awaitable[AgentReply]]) -> None:
        self._process = process
        self.mailbox: asyncio.Queue[tuple[str, asyncio.Future]] = asyncio.Queue()
        self.task = asyncio.create_task(self._loop())
        self.restarts = 0

    async def _loop(self) -> None:
        while True:
            content, reply_future = await self.mailbox.get()
            try:
                reply = await self._process(content)
                if not reply_future.done():
                    reply_future.set_result(reply)
            except asyncio.CancelledError:
                raise
            except Exception as e:
                # supervision: ошибка уходит вызывающему, актор живёт дальше
                self.restarts += 1
                if not reply_future.done():
                    reply_future.set_exception(e)

    def stop(self) -> None:
        self.task.cancel()


class AgentService(AgentPort):
    def __init__(
        self,
        inference: InferencePort,
        context: ContextPort | None = None,
        tools: ToolPort | None = None,
        memory: MemoryPort | None = None,
        sessions: SessionPort | None = None,
        events: EventPort | None = None,
    ) -> None:
        self._inference = inference
        self._context = context
        self._tools = tools
        self._memory = memory
        self._sessions = sessions
        self._events = events
        self._definitions: dict[str, AgentDefinition] = {}
        self._actors: dict[tuple[str, str], _Actor] = {}
        self._reasoners: dict[str, ReasonerFn] = {"tool-loop": tool_loop_reasoner}

    # --- реестр ----------------------------------------------------------------

    def register(self, definition: AgentDefinition) -> None:
        self._definitions[definition.id] = definition

    def register_reasoner(self, name: str, fn: ReasonerFn) -> None:
        """Точка расширения: новая стратегия рассуждения."""
        self._reasoners[name] = fn

    def definitions(self) -> list[AgentDefinition]:
        return list(self._definitions.values())

    # --- обмен сообщениями -------------------------------------------------------

    async def send(self, agent_id: str, content: str, *, session_id: str = "") -> AgentReply:
        if agent_id not in self._definitions:
            raise AgentOSError(f"агент не зарегистрирован: {agent_id}")
        actor = self._get_actor(agent_id, session_id)
        future: asyncio.Future = asyncio.get_running_loop().create_future()
        await actor.mailbox.put((content, future))
        return await future

    async def stop_instance(self, agent_id: str, session_id: str = "") -> None:
        actor = self._actors.pop((agent_id, session_id), None)
        if actor:
            actor.stop()

    async def stop_all(self) -> None:
        for actor in self._actors.values():
            actor.stop()
        self._actors.clear()

    # --- ход агента -----------------------------------------------------------------

    def _get_actor(self, agent_id: str, session_id: str) -> _Actor:
        key = (agent_id, session_id)
        if key not in self._actors:
            self._actors[key] = _Actor(
                lambda content: self._turn(agent_id, session_id, content)
            )
            if self._events:
                asyncio.ensure_future(
                    self._events.publish(
                        Event(type="agent.spawned",
                              payload={"agent_id": agent_id}, subject=session_id)
                    )
                )
        return self._actors[key]

    async def _turn(self, agent_id: str, session_id: str, content: str) -> AgentReply:
        d = self._definitions[agent_id]
        scope = d.memory_scope or f"agent:{agent_id}"

        if self._sessions and session_id:
            await self._sessions.append_turn(
                session_id, Turn(role="user", content=content)
            )

        # 1. Автоматическая сборка контекста под бюджет
        context_text = ""
        if self._context:
            bundle = await self._context.build(
                ContextRequest(
                    intent=content,
                    session_id=session_id,
                    agent_id=agent_id,
                    scope=scope,
                    budget_tokens=d.context_budget,
                )
            )
            context_text = bundle.text()

        # 2. Композиция system-промпта из компонентов агента
        goals = "\n".join(f"- {g}" for g in d.goals)
        system = d.personality
        if goals:
            system += f"\n\nЦели:\n{goals}"
        if context_text:
            system += f"\n\n# Контекст\n{context_text}"

        messages = [ChatMessage(role="system", content=system),
                    ChatMessage(role="user", content=content)]
        tool_schemas: tuple[ToolSchema, ...] = ()
        if self._tools:
            tool_schemas = tuple(
                s for s in self._tools.schemas() if not d.tools or s.name in d.tools
            )

        # 3. Reasoner-цикл (заменяемая стратегия)
        reasoner = self._reasoners.get(d.reasoner)
        if reasoner is None:
            raise AgentOSError(f"reasoner не зарегистрирован: {d.reasoner}")
        reply = await reasoner(
            _TurnEnv(
                definition=d,
                messages=messages,
                tools=tool_schemas,
                inference=self._inference,
                tool_port=self._tools,
                session_id=session_id,
            )
        )

        # 4. Итог хода — в диалог и память
        if self._sessions and session_id:
            await self._sessions.append_turn(
                session_id, Turn(role="assistant", content=reply.text, agent_id=agent_id)
            )
        if self._memory:
            await self._memory.remember(
                MemoryItem(
                    text=f"Вопрос: {content}\nОтвет: {reply.text}",
                    scope=scope,
                    importance=0.5,
                    metadata={"session_id": session_id},
                )
            )
        if self._events:
            await self._events.publish(
                Event(
                    type="agent.turn.completed",
                    payload={
                        "agent_id": agent_id,
                        "iterations": reply.iterations,
                        "tool_calls": reply.tool_calls,
                    },
                    subject=session_id,
                )
            )
        return reply


class AgentRuntime(RuntimeModule):
    def __init__(self) -> None:
        self._service: AgentService | None = None

    def manifest(self) -> ModuleManifest:
        return ModuleManifest(
            id="agent-runtime",
            version="0.2.0",
            provides_ports=("AgentPort@1",),
            requires_ports=("InferencePort@1", "EventPort@1"),
            provides_events=("agent.spawned@1", "agent.turn.completed@1"),
        )

    async def init(self, ctx: ModuleContext) -> None:
        def optional(spec: str):
            try:
                return ctx.port(spec)
            except Exception:
                return None

        self._service = AgentService(
            inference=ctx.port("InferencePort@1"),
            context=optional("ContextPort@1"),
            tools=optional("ToolPort@1"),
            memory=optional("MemoryPort@1"),
            sessions=optional("SessionPort@1"),
            events=ctx.port("EventPort@1"),
        )
        ctx.register("AgentPort@1", self._service)

    async def stop(self, deadline_seconds: float = 10.0) -> None:
        if self._service:
            await self._service.stop_all()

    @property
    def service(self) -> AgentService:
        assert self._service
        return self._service
