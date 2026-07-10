"""Observability Runtime — метрики и наблюдаемость по построению.

Пассивный подписчик шины: модули получают наблюдаемость бесплатно,
публикуя события. Счётчики по типам событий, домен-метрики инференса
(токены, стоимость, латентность по моделям), снапшот для Dashboard/API.
Экспортёры (Prometheus/OTLP) — адаптеры-плагины.
"""

from __future__ import annotations

import time
from collections import defaultdict
from typing import Any

from agentos.contracts.events import Event, EventPort
from agentos.contracts.module import ModuleContext, ModuleManifest, RuntimeModule


class MetricsService:
    def __init__(self) -> None:
        self._started = time.time()
        self.event_counts: dict[str, int] = defaultdict(int)
        self.tokens_by_model: dict[str, int] = defaultdict(int)
        self.cost_by_model: dict[str, float] = defaultdict(float)
        self.tool_calls: dict[str, int] = defaultdict(int)
        self.tool_failures: dict[str, int] = defaultdict(int)

    def attach(self, events: EventPort) -> None:
        events.subscribe("**", self._on_any)

    async def _on_any(self, event: Event) -> None:
        self.event_counts[event.type] += 1
        if event.type == "inference.completed":
            model = event.payload.get("model", "unknown")
            self.tokens_by_model[model] += event.payload.get(
                "prompt_tokens", 0
            ) + event.payload.get("completion_tokens", 0)
            self.cost_by_model[model] += event.payload.get("cost", 0.0)
        elif event.type == "tool.completed":
            self.tool_calls[event.payload.get("tool", "unknown")] += 1
        elif event.type == "tool.failed":
            self.tool_failures[event.payload.get("tool", "unknown")] += 1

    def snapshot(self) -> dict[str, Any]:
        return {
            "uptime_seconds": round(time.time() - self._started, 1),
            "events": dict(self.event_counts),
            "inference": {
                "tokens_by_model": dict(self.tokens_by_model),
                "cost_by_model": {k: round(v, 6) for k, v in self.cost_by_model.items()},
            },
            "tools": {
                "calls": dict(self.tool_calls),
                "failures": dict(self.tool_failures),
            },
        }


class ObservabilityRuntime(RuntimeModule):
    def __init__(self) -> None:
        self._metrics = MetricsService()

    def manifest(self) -> ModuleManifest:
        return ModuleManifest(
            id="observability-runtime",
            version="0.2.0",
            provides_ports=("MetricsPort@1",),
            requires_ports=("EventPort@1",),
        )

    async def init(self, ctx: ModuleContext) -> None:
        self._metrics.attach(ctx.port("EventPort@1"))
        ctx.register("MetricsPort@1", self._metrics)

    @property
    def metrics(self) -> MetricsService:
        return self._metrics
