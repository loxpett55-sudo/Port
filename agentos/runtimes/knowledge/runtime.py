"""Knowledge Runtime — RAG-конвейер: чанкинг → эмбеддинги → индекс →
семантический поиск с ранжированием релевантность × доверие.

Каждый чанк несёт полный провенанс (документ, источник, trust, версия KB).
Версия KB инкрементируется при каждом изменении — запросы воспроизводимы
относительно версии.
"""

from __future__ import annotations

import re

from agentos.contracts.events import Event, EventPort
from agentos.contracts.inference import InferencePort
from agentos.contracts.knowledge import (
    TRUST_WEIGHT,
    Document,
    KnowledgeHit,
    KnowledgePort,
    TrustLevel,
)
from agentos.contracts.model import CAP_EMBED, ModelRequirements
from agentos.contracts.module import ModuleContext, ModuleManifest, RuntimeModule
from agentos.contracts.storage import KVStorePort, VectorItem, VectorStorePort

_NS = "knowledge"
_VNS = "knowledge-vectors"
_EMBED_REQ = ModelRequirements(capabilities=frozenset({CAP_EMBED}))


def chunk_text(text: str, *, max_chars: int = 1200, overlap: int = 120) -> list[str]:
    """Чанкинг по абзацам с ограничением размера и перекрытием.
    Альтернативные стратегии — адаптеры ChunkerPort (плагины)."""
    paragraphs = [p.strip() for p in re.split(r"\n\s*\n", text) if p.strip()]
    chunks: list[str] = []
    current = ""
    for p in paragraphs:
        if len(current) + len(p) + 2 <= max_chars:
            current = f"{current}\n\n{p}".strip()
        else:
            if current:
                chunks.append(current)
            while len(p) > max_chars:  # огромный абзац — жёсткая нарезка
                chunks.append(p[:max_chars])
                p = p[max_chars - overlap :]
            current = p
    if current:
        chunks.append(current)
    return chunks or ([text] if text.strip() else [])


class KnowledgeService(KnowledgePort):
    def __init__(
        self,
        kv: KVStorePort,
        vectors: VectorStorePort,
        inference: InferencePort,
        events: EventPort | None = None,
    ) -> None:
        self._kv = kv
        self._vectors = vectors
        self._inference = inference
        self._events = events

    async def add_document(self, kb: str, document: Document) -> int:
        chunks = chunk_text(document.text)
        if not chunks:
            return 0
        version = await self._bump_version(kb)
        embeddings = await self._inference.embed(chunks, _EMBED_REQ)
        items, index = [], []
        for i, (chunk, vector) in enumerate(zip(chunks, embeddings)):
            chunk_id = f"{document.id}:{i}"
            await self._kv.put(
                _NS,
                f"{kb}:chunk:{chunk_id}",
                {
                    "text": chunk,
                    "document_id": document.id,
                    "source": document.source,
                    "trust": document.trust.value,
                    "kb_version": version,
                    "chunk_index": i,
                },
            )
            index.append(chunk_id)
            items.append(
                VectorItem(id=chunk_id, vector=vector, metadata={"kb": kb})
            )
        await self._vectors.upsert(_VNS, items)
        await self._kv.put(_NS, f"{kb}:doc:{document.id}", index)
        if self._events:
            await self._events.publish(
                Event(
                    type="knowledge.document.indexed",
                    payload={"kb": kb, "document_id": document.id, "chunks": len(chunks)},
                    subject=f"kb:{kb}",
                )
            )
        return len(chunks)

    async def remove_document(self, kb: str, document_id: str) -> int:
        index: list[str] = await self._kv.get(_NS, f"{kb}:doc:{document_id}") or []
        for chunk_id in index:
            await self._kv.delete(_NS, f"{kb}:chunk:{chunk_id}")
        await self._vectors.delete(_VNS, index)
        await self._kv.delete(_NS, f"{kb}:doc:{document_id}")
        if index:
            await self._bump_version(kb)
        return len(index)

    async def retrieve(
        self, query: str, *, kbs: tuple[str, ...] = (), k: int = 8
    ) -> list[KnowledgeHit]:
        [qvec] = await self._inference.embed([query], _EMBED_REQ)
        raw_hits = await self._vectors.search(_VNS, qvec, k=k * 3)
        out: list[KnowledgeHit] = []
        for hit in raw_hits:
            kb = hit.metadata.get("kb", "")
            if kbs and kb not in kbs:
                continue
            chunk = await self._kv.get(_NS, f"{kb}:chunk:{hit.id}")
            if chunk is None:
                continue
            trust = TrustLevel(chunk["trust"])
            out.append(
                KnowledgeHit(
                    text=chunk["text"],
                    score=hit.score * TRUST_WEIGHT[trust],  # релевантность × доверие
                    document_id=chunk["document_id"],
                    source=chunk["source"],
                    trust=trust,
                    kb=kb,
                    kb_version=chunk["kb_version"],
                    chunk_index=chunk["chunk_index"],
                )
            )
        out.sort(key=lambda h: h.score, reverse=True)
        return out[:k]

    async def version(self, kb: str) -> int:
        return await self._kv.get(_NS, f"{kb}:version") or 0

    async def _bump_version(self, kb: str) -> int:
        version = (await self._kv.get(_NS, f"{kb}:version") or 0) + 1
        await self._kv.put(_NS, f"{kb}:version", version)
        return version


class KnowledgeRuntime(RuntimeModule):
    def __init__(self) -> None:
        self._service: KnowledgeService | None = None

    def manifest(self) -> ModuleManifest:
        return ModuleManifest(
            id="knowledge-runtime",
            version="0.2.0",
            provides_ports=("KnowledgePort@1",),
            requires_ports=(
                "KVStorePort@1",
                "VectorStorePort@1",
                "InferencePort@1",
                "EventPort@1",
            ),
            provides_events=("knowledge.document.indexed@1",),
        )

    async def init(self, ctx: ModuleContext) -> None:
        self._service = KnowledgeService(
            ctx.port("KVStorePort@1"),
            ctx.port("VectorStorePort@1"),
            ctx.port("InferencePort@1"),
            ctx.port("EventPort@1"),
        )
        ctx.register("KnowledgePort@1", self._service)
