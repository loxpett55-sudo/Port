"""Memory Runtime — многоуровневая память.

Записи хранятся в KV (данные) + VectorStore (поиск). Жизненный цикл:
ingestion → Short-Term → консолидация → Long-Term → архив/забвение.
Scope-изоляция: каждый item принадлежит scope (user:/agent:/global),
recall никогда не пересекает scope.
"""

from __future__ import annotations

import time
from dataclasses import asdict

from agentos.contracts.events import Event, EventPort
from agentos.contracts.inference import InferencePort
from agentos.contracts.memory import MemoryHit, MemoryItem, MemoryLevel, MemoryPort
from agentos.contracts.model import CAP_EMBED, ModelRequirements
from agentos.contracts.module import ModuleContext, ModuleManifest, RuntimeModule
from agentos.contracts.storage import KVStorePort, VectorItem, VectorStorePort

_NS = "memory"
_VNS = "memory-vectors"
_EMBED_REQ = ModelRequirements(capabilities=frozenset({CAP_EMBED}))


class MemoryService(MemoryPort):
    def __init__(
        self,
        kv: KVStorePort,
        vectors: VectorStorePort,
        inference: InferencePort,
        events: EventPort | None = None,
        *,
        promote_threshold: float = 0.6,
    ) -> None:
        self._kv = kv
        self._vectors = vectors
        self._inference = inference
        self._events = events
        self._promote_threshold = promote_threshold

    async def remember(self, item: MemoryItem) -> str:
        await self._save(item)
        [vector] = await self._inference.embed([item.text], _EMBED_REQ)
        await self._vectors.upsert(
            _VNS,
            [
                VectorItem(
                    id=item.id,
                    vector=vector,
                    metadata={"scope": item.scope, "level": item.level.value},
                )
            ],
        )
        await self._emit("memory.item.stored", item)
        return item.id

    async def recall(
        self,
        query: str,
        scope: str,
        *,
        levels: tuple[MemoryLevel, ...] = (),
        k: int = 8,
    ) -> list[MemoryHit]:
        [qvec] = await self._inference.embed([query], _EMBED_REQ)
        hits = await self._vectors.search(_VNS, qvec, k=k * 3, where={"scope": scope})
        wanted = {l.value for l in levels} if levels else None
        out: list[MemoryHit] = []
        for hit in hits:
            if wanted and hit.metadata.get("level") not in wanted:
                continue
            raw = await self._kv.get(_NS, f"{scope}:{hit.id}")
            if raw is None:
                continue
            item = _item_from_dict(raw)
            item.last_access = time.time()
            item.access_count += 1
            await self._save(item)
            out.append(MemoryHit(item=item, score=hit.score))
            if len(out) >= k:
                break
        return out

    async def forget(self, scope: str, item_id: str) -> bool:
        deleted = await self._kv.delete(_NS, f"{scope}:{item_id}")
        await self._vectors.delete(_VNS, [item_id])
        if deleted and self._events:
            await self._events.publish(
                Event(type="memory.item.forgotten", payload={"id": item_id}, subject=scope)
            )
        return deleted

    async def consolidate(self, scope: str) -> int:
        """Short-Term → Long-Term: значимость + востребованность."""
        promoted = 0
        for item in await self._items(scope):
            if item.level is not MemoryLevel.SHORT_TERM:
                continue
            score = item.importance + min(item.access_count, 5) * 0.1
            if score >= self._promote_threshold:
                item.level = MemoryLevel.LONG_TERM
                await self._save(item)
                await self._reindex_level(item)
                await self._emit("memory.item.promoted", item)
                promoted += 1
        return promoted

    async def archive(self, scope: str, *, max_idle_seconds: float) -> int:
        """Long-Term → Archive для невостребованных записей."""
        archived = 0
        cutoff = time.time() - max_idle_seconds
        for item in await self._items(scope):
            if item.level is MemoryLevel.LONG_TERM and item.last_access < cutoff:
                item.level = MemoryLevel.ARCHIVE
                await self._save(item)
                await self._reindex_level(item)
                await self._emit("memory.item.archived", item)
                archived += 1
        return archived

    # --- внутреннее -------------------------------------------------------

    async def _items(self, scope: str) -> list[MemoryItem]:
        out = []
        for key in await self._kv.keys(_NS, prefix=f"{scope}:"):
            raw = await self._kv.get(_NS, key)
            if raw:
                out.append(_item_from_dict(raw))
        return out

    async def _save(self, item: MemoryItem) -> None:
        data = asdict(item)
        data["level"] = item.level.value
        await self._kv.put(_NS, f"{item.scope}:{item.id}", data)

    async def _reindex_level(self, item: MemoryItem) -> None:
        [vector] = await self._inference.embed([item.text], _EMBED_REQ)
        await self._vectors.upsert(
            _VNS,
            [
                VectorItem(
                    id=item.id,
                    vector=vector,
                    metadata={"scope": item.scope, "level": item.level.value},
                )
            ],
        )

    async def _emit(self, type_: str, item: MemoryItem) -> None:
        if self._events:
            await self._events.publish(
                Event(
                    type=type_,
                    payload={"id": item.id, "level": item.level.value},
                    subject=item.scope,
                )
            )


def _item_from_dict(raw: dict) -> MemoryItem:
    raw = dict(raw)
    raw["level"] = MemoryLevel(raw["level"])
    return MemoryItem(**raw)


class MemoryRuntime(RuntimeModule):
    def __init__(self) -> None:
        self._service: MemoryService | None = None

    def manifest(self) -> ModuleManifest:
        return ModuleManifest(
            id="memory-runtime",
            version="0.2.0",
            provides_ports=("MemoryPort@1",),
            requires_ports=(
                "KVStorePort@1",
                "VectorStorePort@1",
                "InferencePort@1",
                "EventPort@1",
            ),
            provides_events=(
                "memory.item.stored@1",
                "memory.item.promoted@1",
                "memory.item.archived@1",
                "memory.item.forgotten@1",
            ),
            config_schema={"promote_threshold": {"type": "number"}},
        )

    async def init(self, ctx: ModuleContext) -> None:
        self._service = MemoryService(
            ctx.port("KVStorePort@1"),
            ctx.port("VectorStorePort@1"),
            ctx.port("InferencePort@1"),
            ctx.port("EventPort@1"),
            promote_threshold=float(ctx.config.get("promote_threshold", 0.6)),
        )
        ctx.register("MemoryPort@1", self._service)
