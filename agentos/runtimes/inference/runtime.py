"""Inference Runtime — надёжное исполнение запросов к моделям.

Очереди с приоритетами, ограничение параллелизма, ретраи с
экспоненциальной задержкой, кэш результатов, стриминг, батчинг
эмбеддингов, события учёта токенов/стоимости.
"""

from __future__ import annotations

import asyncio
import hashlib
import heapq
import itertools
import json
from dataclasses import asdict
from typing import AsyncIterator

from agentos.contracts.errors import AgentOSError
from agentos.contracts.events import Event, EventPort
from agentos.contracts.inference import InferencePort, InferenceRequest
from agentos.contracts.model import (
    GenerateChunk,
    GenerateRequest,
    GenerateResult,
    ModelPort,
    ModelRegistryPort,
    ModelRequirements,
    ToolCall,
    Usage,
)
from agentos.contracts.module import ModuleContext, ModuleManifest, RuntimeModule
from agentos.contracts.storage import KVStorePort

_CACHE_NS = "inference-cache"


class PriorityGate:
    """Ограничитель параллелизма, отдающий слоты по приоритету."""

    def __init__(self, slots: int) -> None:
        self._free = slots
        self._waiters: list[tuple[int, int, asyncio.Future]] = []
        self._seq = itertools.count()

    async def acquire(self, priority: int) -> None:
        if self._free > 0 and not self._waiters:
            self._free -= 1
            return
        fut: asyncio.Future = asyncio.get_running_loop().create_future()
        heapq.heappush(self._waiters, (priority, next(self._seq), fut))
        await fut

    def release(self) -> None:
        while self._waiters:
            _, _, fut = heapq.heappop(self._waiters)
            if not fut.done():
                fut.set_result(None)
                return
        self._free += 1


class InferenceEngine(InferencePort):
    def __init__(
        self,
        models: ModelRegistryPort,
        kv: KVStorePort | None = None,
        events: EventPort | None = None,
        *,
        max_concurrency: int = 4,
        base_backoff: float = 0.05,
        cache_ttl: float = 3600.0,
        embed_batch_size: int = 64,
    ) -> None:
        self._models = models
        self._kv = kv
        self._events = events
        self._gate = PriorityGate(max_concurrency)
        self._base_backoff = base_backoff
        self._cache_ttl = cache_ttl
        self._embed_batch = embed_batch_size

    # --- публичный порт ---------------------------------------------------

    async def generate(self, req: InferenceRequest) -> GenerateResult:
        model = self._models.select(req.requirements)
        cache_key = self._cache_key(model, req.request)

        if req.use_cache and self._kv and req.request.temperature == 0.0:
            cached = await self._kv.get(_CACHE_NS, cache_key)
            if cached is not None:
                await self._emit(req, model, Usage(), cached=True)
                return _result_from_dict(cached)

        result = await self._with_retries(req, model, self._collect)

        if req.use_cache and self._kv and req.request.temperature == 0.0:
            await self._kv.put(_CACHE_NS, cache_key, asdict(result), ttl=self._cache_ttl)
        await self._emit(req, model, result.usage)
        return result

    async def stream(self, req: InferenceRequest) -> AsyncIterator[GenerateChunk]:
        model = self._models.select(req.requirements)
        await self._gate.acquire(int(req.priority))
        try:
            usage = Usage()
            async for chunk in model.generate(req.request):
                if chunk.usage:
                    usage = chunk.usage
                yield chunk
            await self._emit(req, model, usage)
        finally:
            self._gate.release()

    async def embed(
        self, texts: list[str], requirements: ModelRequirements
    ) -> list[list[float]]:
        model = self._models.select(requirements)
        out: list[list[float]] = []
        for i in range(0, len(texts), self._embed_batch):
            out.extend(await model.embed(texts[i : i + self._embed_batch]))
        return out

    # --- внутреннее -------------------------------------------------------

    async def _with_retries(self, req, model: ModelPort, op) -> GenerateResult:
        last: Exception | None = None
        for attempt in range(req.max_attempts):
            await self._gate.acquire(int(req.priority))
            try:
                return await op(model, req.request)
            except AgentOSError as e:
                last = e
                if not e.retryable:
                    raise
                await asyncio.sleep(self._base_backoff * (2**attempt))
            finally:
                self._gate.release()
        if self._events:
            await self._events.publish(
                Event(
                    type="inference.failed",
                    payload={"model": model.descriptor().id, "error": str(last)},
                    subject=req.session_id,
                )
            )
        raise last  # type: ignore[misc]

    @staticmethod
    async def _collect(model: ModelPort, request: GenerateRequest) -> GenerateResult:
        text_parts: list[str] = []
        tool_calls: list[ToolCall] = []
        usage = Usage()
        finish = "stop"
        async for chunk in model.generate(request):
            if chunk.delta:
                text_parts.append(chunk.delta)
            if chunk.tool_call:
                tool_calls.append(chunk.tool_call)
            if chunk.finish_reason:
                finish = chunk.finish_reason
            if chunk.usage:
                usage = chunk.usage
        return GenerateResult(
            text="".join(text_parts),
            tool_calls=tuple(tool_calls),
            finish_reason=finish,
            usage=usage,
        )

    @staticmethod
    def _cache_key(model: ModelPort, request: GenerateRequest) -> str:
        raw = json.dumps(
            [model.descriptor().id, asdict(request)], sort_keys=True, ensure_ascii=False
        )
        return hashlib.sha256(raw.encode()).hexdigest()

    async def _emit(self, req: InferenceRequest, model: ModelPort, usage: Usage, cached=False):
        if not self._events:
            return
        await self._events.publish(
            Event(
                type="inference.completed",
                payload={
                    "model": model.descriptor().id,
                    "prompt_tokens": usage.prompt_tokens,
                    "completion_tokens": usage.completion_tokens,
                    "cost": usage.cost,
                    "cached": cached,
                    "agent_id": req.agent_id,
                },
                subject=req.session_id,
                priority=req.priority,
            )
        )


def _result_from_dict(d: dict) -> GenerateResult:
    return GenerateResult(
        text=d["text"],
        tool_calls=tuple(ToolCall(**tc) for tc in d.get("tool_calls", [])),
        finish_reason=d.get("finish_reason", "stop"),
        usage=Usage(**d.get("usage", {})),
    )


class InferenceRuntime(RuntimeModule):
    def __init__(self) -> None:
        self._engine: InferenceEngine | None = None

    def manifest(self) -> ModuleManifest:
        return ModuleManifest(
            id="inference-runtime",
            version="0.2.0",
            provides_ports=("InferencePort@1",),
            requires_ports=("ModelRegistryPort@1", "KVStorePort@1", "EventPort@1"),
            provides_events=("inference.completed@1", "inference.failed@1"),
            config_schema={"max_concurrency": {"type": "integer", "minimum": 1}},
        )

    async def init(self, ctx: ModuleContext) -> None:
        self._engine = InferenceEngine(
            models=ctx.port("ModelRegistryPort@1"),
            kv=ctx.port("KVStorePort@1"),
            events=ctx.port("EventPort@1"),
            max_concurrency=int(ctx.config.get("max_concurrency", 4)),
        )
        ctx.register("InferencePort@1", self._engine)

    @property
    def engine(self) -> InferenceEngine:
        assert self._engine is not None
        return self._engine
