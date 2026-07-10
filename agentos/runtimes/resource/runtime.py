"""Resource Runtime — бюджеты токенов/стоимости/вызовов инструментов.

Учёт пассивный: подписка на inference.completed и tool.completed —
наблюдаемость не требует инструментирования потребителей. Enforcement
активный: check(scope) на границах. Бюджет контекста выдаётся из
остатка токенов (деградация вместо отказа).
"""

from __future__ import annotations

from agentos.contracts.errors import ResourceExhaustedError
from agentos.contracts.events import Event, EventPort
from agentos.contracts.module import ModuleContext, ModuleManifest, RuntimeModule
from agentos.contracts.resource import Budget, ResourcePort, ResourceUsage


class ResourceService(ResourcePort):
    def __init__(self, events: EventPort | None = None, *, warn_ratio: float = 0.8):
        self._events = events
        self._budgets: dict[str, Budget] = {}
        self._usage: dict[str, ResourceUsage] = {}
        self._warned: set[str] = set()

    def attach(self, events: EventPort) -> None:
        """Пассивный учёт: consume по событиям инференса и инструментов."""
        events.subscribe("inference.completed", self._on_inference)
        events.subscribe("tool.completed", self._on_tool)

    async def _on_inference(self, event: Event) -> None:
        scope = event.subject or "global"
        tokens = event.payload.get("prompt_tokens", 0) + event.payload.get(
            "completion_tokens", 0
        )
        await self._consume(scope, tokens=tokens, cost=event.payload.get("cost", 0.0))

    async def _on_tool(self, event: Event) -> None:
        await self._consume(event.subject or "global", tool_calls=1)

    # --- ResourcePort ---------------------------------------------------------

    def set_budget(self, scope: str, budget: Budget) -> None:
        self._budgets[scope] = budget

    def usage(self, scope: str) -> ResourceUsage:
        return self._usage.setdefault(scope, ResourceUsage())

    def check(self, scope: str) -> None:
        budget = self._budgets.get(scope)
        if budget is None:
            return
        usage = self.usage(scope)
        if budget.max_tokens and usage.tokens >= budget.max_tokens:
            raise ResourceExhaustedError(
                f"{scope}: бюджет токенов исчерпан ({usage.tokens}/{budget.max_tokens})"
            )
        if budget.max_cost and usage.cost >= budget.max_cost:
            raise ResourceExhaustedError(
                f"{scope}: бюджет стоимости исчерпан ({usage.cost:.4f}/{budget.max_cost})"
            )
        if budget.max_tool_calls and usage.tool_calls >= budget.max_tool_calls:
            raise ResourceExhaustedError(
                f"{scope}: бюджет вызовов инструментов исчерпан"
            )

    def consume(self, scope: str, *, tokens: int = 0, cost: float = 0.0, tool_calls: int = 0):
        import asyncio

        asyncio.ensure_future(
            self._consume(scope, tokens=tokens, cost=cost, tool_calls=tool_calls)
        )

    async def _consume(
        self, scope: str, *, tokens: int = 0, cost: float = 0.0, tool_calls: int = 0
    ) -> None:
        usage = self.usage(scope)
        usage.tokens += tokens
        usage.cost += cost
        usage.tool_calls += tool_calls
        budget = self._budgets.get(scope)
        if budget and budget.max_tokens and self._events and scope not in self._warned:
            if usage.tokens >= budget.max_tokens * 0.8:
                self._warned.add(scope)
                await self._events.publish(
                    Event(
                        type="budget.threshold.crossed",
                        payload={"scope": scope, "tokens": usage.tokens,
                                 "max_tokens": budget.max_tokens},
                        subject=scope,
                    )
                )

    def context_budget(self, scope: str, requested: int) -> int:
        budget = self._budgets.get(scope)
        if budget is None or not budget.max_tokens:
            return requested
        remaining = budget.max_tokens - self.usage(scope).tokens
        return max(0, min(requested, remaining))


class ResourceRuntime(RuntimeModule):
    def __init__(self) -> None:
        self._service: ResourceService | None = None

    def manifest(self) -> ModuleManifest:
        return ModuleManifest(
            id="resource-runtime",
            version="0.2.0",
            provides_ports=("ResourcePort@1",),
            requires_ports=("EventPort@1",),
            provides_events=("budget.threshold.crossed@1",),
        )

    async def init(self, ctx: ModuleContext) -> None:
        self._service = ResourceService(ctx.port("EventPort@1"))
        self._service.attach(ctx.port("EventPort@1"))
        ctx.register("ResourcePort@1", self._service)

    @property
    def service(self) -> ResourceService:
        assert self._service
        return self._service
