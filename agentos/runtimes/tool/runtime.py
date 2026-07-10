"""Tool Runtime — инструменты как управляемые сервисы.

Конвейер вызова: политика → валидация входа → исполнение с тайм-аутом →
валидация выхода → события аудита и метрик. Sandbox-уровни process/
container подключаются адаптерами Security Runtime; здесь — enforcement
тайм-аутов и прав.
"""

from __future__ import annotations

import time
from typing import Any

from agentos.contracts.errors import PermissionDeniedError, PortNotFoundError
from agentos.contracts.events import Event, EventPort
from agentos.contracts.model import ToolSchema
from agentos.contracts.module import ModuleContext, ModuleManifest, RuntimeModule
from agentos.contracts.tool import (
    ToolCallContext,
    ToolHandler,
    ToolManifest,
    ToolPort,
    ToolResult,
)
from agentos.lib.jsonschema import validate

import asyncio


class ToolRegistry(ToolPort):
    def __init__(self, events: EventPort | None = None, policy: Any | None = None) -> None:
        self._tools: dict[str, tuple[ToolManifest, ToolHandler]] = {}
        self._events = events
        self._policy = policy  # PolicyPort (появится в Этапе 6); duck-typed

    def register(self, manifest: ToolManifest, handler: ToolHandler) -> None:
        self._tools[manifest.id] = (manifest, handler)

    def unregister(self, tool_id: str) -> None:
        self._tools.pop(tool_id, None)

    def list(self) -> list[ToolManifest]:
        return [m for m, _ in self._tools.values()]

    def get(self, tool_id: str) -> ToolManifest:
        try:
            return self._tools[tool_id][0]
        except KeyError:
            raise PortNotFoundError(f"инструмент не зарегистрирован: {tool_id}") from None

    def schemas(self) -> list[ToolSchema]:
        """Tool Context для моделей (потребляет Context Runtime)."""
        return [
            ToolSchema(name=m.id, description=m.description, input_schema=m.input_schema)
            for m, _ in self._tools.values()
        ]

    async def invoke(
        self, tool_id: str, arguments: dict[str, Any], context: ToolCallContext
    ) -> ToolResult:
        manifest, handler = self._tools.get(tool_id, (None, None))
        started = time.monotonic()

        if manifest is None or handler is None:
            return await self._finish(
                tool_id, context, started, ok=False, error=f"инструмент не найден: {tool_id}"
            )

        # 1. Политика: разрешён ли вызов этому агенту/сессии
        if self._policy is not None:
            decision = await self._policy.evaluate(
                action="tool.invoke",
                subject={"agent_id": context.agent_id, "principal": context.principal},
                resource={"tool_id": tool_id, "permissions": list(manifest.permissions)},
            )
            if not decision.allowed:
                await self._emit("tool.denied", tool_id, context, reason=decision.reason)
                raise PermissionDeniedError(
                    f"вызов {tool_id} запрещён политикой: {decision.reason}"
                )

        # 2. Валидация входа по схеме манифеста
        errors = validate(manifest.input_schema, arguments)
        if errors:
            return await self._finish(
                tool_id, context, started, ok=False, error="невалидный вход: " + "; ".join(errors)
            )

        # 3. Исполнение с тайм-аутом
        try:
            output = await asyncio.wait_for(
                handler(arguments, context), timeout=manifest.timeout_seconds
            )
        except asyncio.TimeoutError:
            return await self._finish(
                tool_id, context, started, ok=False,
                error=f"тайм-аут {manifest.timeout_seconds}s",
            )
        except Exception as e:
            return await self._finish(tool_id, context, started, ok=False, error=str(e))

        # 4. Валидация выхода (если схема задана)
        if manifest.output_schema:
            errors = validate(manifest.output_schema, output)
            if errors:
                return await self._finish(
                    tool_id, context, started, ok=False,
                    error="невалидный выход: " + "; ".join(errors),
                )

        return await self._finish(tool_id, context, started, ok=True, output=output)

    async def _finish(
        self, tool_id, context, started, *, ok, output=None, error=""
    ) -> ToolResult:
        duration = time.monotonic() - started
        await self._emit(
            "tool.completed" if ok else "tool.failed",
            tool_id,
            context,
            duration=duration,
            error=error,
        )
        return ToolResult(ok=ok, output=output, error=error, duration_seconds=duration)

    async def _emit(self, type_: str, tool_id: str, context: ToolCallContext, **extra):
        if self._events is None:
            return
        await self._events.publish(
            Event(
                type=type_,
                payload={"tool": tool_id, "agent_id": context.agent_id, **extra},
                subject=context.session_id,
                correlation_id=context.correlation_id,
            )
        )


class ToolRuntime(RuntimeModule):
    def __init__(self) -> None:
        self._registry: ToolRegistry | None = None

    def manifest(self) -> ModuleManifest:
        return ModuleManifest(
            id="tool-runtime",
            version="0.2.0",
            provides_ports=("ToolPort@1",),
            requires_ports=("EventPort@1",),
            optional_ports=("PolicyPort@1",),
            provides_events=("tool.completed@1", "tool.failed@1", "tool.denied@1"),
        )

    async def init(self, ctx: ModuleContext) -> None:
        events = ctx.port("EventPort@1")
        policy = None
        try:
            policy = ctx.port("PolicyPort@1")  # опционально до Этапа 6
        except Exception:
            pass
        self._registry = ToolRegistry(events, policy)
        ctx.register("ToolPort@1", self._registry)

    @property
    def registry(self) -> ToolRegistry:
        assert self._registry is not None
        return self._registry
