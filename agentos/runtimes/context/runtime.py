"""Context Runtime — автоматическая сборка контекста под бюджет.

Конвейер: Relevance Planner (какие источники и доли бюджета) →
Fetchers (память, знания, диалог, инструменты) → Compressor
(усечение под бюджет) → Assembler (порядок секций). Каждая секция
несёт провенанс.
"""

from __future__ import annotations

from typing import Any

from agentos.contracts.context import (
    ContextBundle,
    ContextPort,
    ContextRequest,
    ContextSection,
    approx_tokens,
)
from agentos.contracts.events import Event, EventPort
from agentos.contracts.knowledge import KnowledgePort
from agentos.contracts.memory import MemoryPort
from agentos.contracts.module import ModuleContext, ModuleManifest, RuntimeModule
from agentos.contracts.session import SessionPort

# Порядок сборки и доли бюджета по умолчанию (Planner корректирует)
_DEFAULT_SHARES = {
    "system": 0.05,
    "tools": 0.10,
    "knowledge": 0.30,
    "memory": 0.20,
    "conversation": 0.35,
}
_SECTION_ORDER = ["system", "tools", "knowledge", "memory", "conversation"]


class ContextBuilder(ContextPort):
    def __init__(
        self,
        sessions: SessionPort | None = None,
        memory: MemoryPort | None = None,
        knowledge: KnowledgePort | None = None,
        tools: Any | None = None,          # ToolRegistry (schemas())
        events: EventPort | None = None,
        system_info: str = "AgentOS Cognitive Runtime Platform",
    ) -> None:
        self._sessions = sessions
        self._memory = memory
        self._knowledge = knowledge
        self._tools = tools
        self._events = events
        self._system_info = system_info

    async def build(self, request: ContextRequest) -> ContextBundle:
        shares = self._plan(request)
        sections: list[ContextSection] = []
        for name in _SECTION_ORDER:
            if name not in shares:
                continue
            budget = int(request.budget_tokens * shares[name])
            if budget <= 0:
                continue
            section = await self._fetch(name, request, budget)
            if section and section.content:
                sections.append(section)

        total = sum(s.tokens for s in sections)
        bundle = ContextBundle(
            sections=tuple(sections),
            total_tokens=total,
            budget_tokens=request.budget_tokens,
        )
        if self._events:
            await self._events.publish(
                Event(
                    type="context.built",
                    payload={
                        "consumer": request.agent_id or "unknown",
                        "sections": {s.name: s.tokens for s in sections},
                        "total_tokens": total,
                        "budget": request.budget_tokens,
                    },
                    subject=request.session_id,
                )
            )
        return bundle

    # --- Planner ------------------------------------------------------------

    def _plan(self, request: ContextRequest) -> dict[str, float]:
        """Runtime сам решает, какие части контекста нужны: явный include
        уважается, иначе — эвристика по доступным источникам."""
        if request.include:
            share = 1.0 / len(request.include)
            return {name: share for name in request.include}
        shares = dict(_DEFAULT_SHARES)
        if self._knowledge is None:
            shares.pop("knowledge", None)
        if self._memory is None:
            shares.pop("memory", None)
        if self._sessions is None or not request.session_id:
            shares.pop("conversation", None)
        if self._tools is None:
            shares.pop("tools", None)
        # нормализация долей
        total = sum(shares.values())
        return {k: v / total for k, v in shares.items()}

    # --- Fetchers + Compressor -----------------------------------------------

    async def _fetch(
        self, name: str, request: ContextRequest, budget: int
    ) -> ContextSection | None:
        if name == "system":
            return _compress("system", self._system_info, budget, ("system",))

        if name == "tools" and self._tools is not None:
            lines = [
                f"- {s.name}: {s.description}" for s in self._tools.schemas()
            ]
            return _compress("tools", "\n".join(lines), budget, ("tool-registry",))

        if name == "conversation" and self._sessions and request.session_id:
            turns = await self._sessions.turns(request.session_id, limit=50)
            lines, provenance = [], (f"session:{request.session_id}",)
            for t in reversed(turns):  # свежие важнее — набираем с конца
                line = f"{t.role}: {t.content}"
                if approx_tokens("\n".join(lines) + line) > budget:
                    break
                lines.insert(0, line)
            return _compress("conversation", "\n".join(lines), budget, provenance)

        if name == "memory" and self._memory and request.scope:
            hits = await self._memory.recall(request.intent, request.scope, k=8)
            lines = [f"- {h.item.text}" for h in hits]
            prov = tuple(f"memory:{h.item.id}" for h in hits)
            return _compress("memory", "\n".join(lines), budget, prov)

        if name == "knowledge" and self._knowledge:
            hits = await self._knowledge.retrieve(request.intent, k=6)
            lines = [
                f"- [{h.source}, trust={h.trust.value}] {h.text}" for h in hits
            ]
            prov = tuple(f"{h.kb}@v{h.kb_version}:{h.document_id}#{h.chunk_index}" for h in hits)
            return _compress("knowledge", "\n".join(lines), budget, prov)

        return None


def _compress(
    name: str, content: str, budget: int, provenance: tuple[str, ...]
) -> ContextSection:
    """Гарантия бюджета: жёсткое усечение. Суммаризация через Inference —
    адаптер-стратегия (плагин), подключается конфигурацией."""
    if approx_tokens(content) > budget:
        content = content[: budget * 4]
    return ContextSection(
        name=name, content=content, tokens=approx_tokens(content), provenance=provenance
    )


class ContextRuntime(RuntimeModule):
    def __init__(self) -> None:
        self._builder: ContextBuilder | None = None

    def manifest(self) -> ModuleManifest:
        return ModuleManifest(
            id="context-runtime",
            version="0.2.0",
            provides_ports=("ContextPort@1",),
            requires_ports=("EventPort@1",),  # остальные источники — опциональны
            provides_events=("context.built@1",),
        )

    async def init(self, ctx: ModuleContext) -> None:
        def optional(spec: str):
            try:
                return ctx.port(spec)
            except Exception:
                return None

        self._builder = ContextBuilder(
            sessions=optional("SessionPort@1"),
            memory=optional("MemoryPort@1"),
            knowledge=optional("KnowledgePort@1"),
            tools=optional("ToolPort@1"),
            events=ctx.port("EventPort@1"),
        )
        ctx.register("ContextPort@1", self._builder)
