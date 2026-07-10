"""Тесты Этапа 4: Session, Memory, Knowledge, Context."""

import asyncio

import pytest

from agentos.adapters.memory_store import InMemoryEventLog, InMemoryKV, InMemoryVectorStore
from agentos.adapters.models.stub import HashEmbedder
from agentos.contracts.context import ContextRequest
from agentos.contracts.errors import AgentOSError
from agentos.contracts.knowledge import Document, TrustLevel
from agentos.contracts.memory import MemoryItem, MemoryLevel
from agentos.contracts.session import SessionState, Turn
from agentos.runtimes.context import ContextBuilder
from agentos.runtimes.event.runtime import EventBus
from agentos.runtimes.inference import InferenceEngine
from agentos.runtimes.knowledge import KnowledgeService
from agentos.runtimes.knowledge.runtime import chunk_text
from agentos.runtimes.memory import MemoryService
from agentos.runtimes.model import ModelRegistry
from agentos.runtimes.session import SessionService


@pytest.fixture
async def bus():
    b = EventBus(InMemoryEventLog())
    await b.start()
    yield b
    await b.stop()


@pytest.fixture
def inference():
    reg = ModelRegistry()
    reg.register(HashEmbedder())
    return InferenceEngine(reg)


# --- Session ----------------------------------------------------------------

async def test_session_lifecycle(bus):
    svc = SessionService(InMemoryKV(), bus)
    session = await svc.open("user:alice")
    assert session.state is SessionState.ACTIVE

    await svc.append_turn(session.id, Turn(role="user", content="привет"))
    await svc.append_turn(session.id, Turn(role="assistant", content="здравствуйте"))
    turns = await svc.turns(session.id)
    assert [t.role for t in turns] == ["user", "assistant"]

    await svc.suspend(session.id)
    assert (await svc.get(session.id)).state is SessionState.SUSPENDED
    await svc.resume(session.id)
    await svc.close(session.id)
    with pytest.raises(AgentOSError, match="закрыта"):
        await svc.append_turn(session.id, Turn(role="user", content="ещё"))

    await bus.drain()
    events = [e.type for e in await bus.history("session.*")]
    assert events == [
        "session.opened", "session.suspended", "session.resumed", "session.closed",
    ]


# --- Memory -------------------------------------------------------------------

async def test_memory_recall_scoped(inference):
    mem = MemoryService(InMemoryKV(), InMemoryVectorStore(), inference)
    await mem.remember(MemoryItem(text="пользователь любит зелёный чай", scope="user:a"))
    await mem.remember(MemoryItem(text="встреча по проекту в пятницу", scope="user:a"))
    await mem.remember(MemoryItem(text="секрет другого пользователя", scope="user:b"))

    hits = await mem.recall("какой чай предпочитает пользователь", "user:a", k=1)
    assert hits and "чай" in hits[0].item.text
    # scope-изоляция: чужие данные недостижимы
    all_a = await mem.recall("секрет", "user:a", k=10)
    assert all(h.item.scope == "user:a" for h in all_a)


async def test_memory_consolidation_and_archive(inference, bus):
    mem = MemoryService(InMemoryKV(), InMemoryVectorStore(), inference, bus)
    important = MemoryItem(text="критично: дедлайн 1 марта", scope="u", importance=0.9)
    trivial = MemoryItem(text="мимолётная реплика", scope="u", importance=0.1)
    await mem.remember(important)
    await mem.remember(trivial)

    promoted = await mem.consolidate("u")
    assert promoted == 1
    hits = await mem.recall("дедлайн", "u", levels=(MemoryLevel.LONG_TERM,), k=5)
    assert [h.item.id for h in hits] == [important.id]

    archived = await mem.archive("u", max_idle_seconds=-1)  # всё бездействует
    assert archived == 1  # только long-term запись
    await bus.drain()
    types = [e.type for e in await bus.history("memory.**")]
    assert "memory.item.promoted" in types and "memory.item.archived" in types


async def test_memory_forget(inference):
    mem = MemoryService(InMemoryKV(), InMemoryVectorStore(), inference)
    item = MemoryItem(text="удалить меня", scope="u")
    await mem.remember(item)
    assert await mem.forget("u", item.id) is True
    assert await mem.recall("удалить", "u", k=5) == []


# --- Knowledge ------------------------------------------------------------------

def test_chunking():
    text = "\n\n".join(f"Абзац номер {i}. " + "слово " * 50 for i in range(10))
    chunks = chunk_text(text, max_chars=600)
    assert len(chunks) > 3
    assert all(len(c) <= 600 for c in chunks)
    assert chunk_text("короткий текст") == ["короткий текст"]


async def test_knowledge_index_retrieve_provenance(inference, bus):
    kb = KnowledgeService(InMemoryKV(), InMemoryVectorStore(), inference, bus)
    n = await kb.add_document(
        "wiki",
        Document(
            text="Платформа AgentOS использует микроядро и событийную шину.",
            source="wiki/architecture.md",
            trust=TrustLevel.VERIFIED,
        ),
    )
    assert n == 1
    await kb.add_document(
        "wiki",
        Document(
            text="Слухи: платформа написана на коболе микроядро шина.",
            source="forum/rumors",
            trust=TrustLevel.UNVERIFIED,
        ),
    )
    hits = await kb.retrieve("что использует микроядро платформа")
    assert hits[0].source == "wiki/architecture.md"  # trust перевешивает
    assert hits[0].kb_version == 1 and hits[0].trust is TrustLevel.VERIFIED
    assert await kb.version("wiki") == 2


async def test_knowledge_remove_document(inference):
    kb = KnowledgeService(InMemoryKV(), InMemoryVectorStore(), inference)
    doc = Document(text="временный документ про тюльпаны", source="tmp")
    await kb.add_document("kb1", doc)
    assert await kb.remove_document("kb1", doc.id) == 1
    assert await kb.retrieve("тюльпаны", kbs=("kb1",)) == []


# --- Context ---------------------------------------------------------------------

class FakeTools:
    def schemas(self):
        from agentos.contracts.model import ToolSchema

        return [ToolSchema(name="search", description="Поиск в вебе", input_schema={})]


async def test_context_build_full(inference, bus):
    kv, vs = InMemoryKV(), InMemoryVectorStore()
    sessions = SessionService(kv, bus)
    memory = MemoryService(kv, vs, inference)
    knowledge = KnowledgeService(kv, vs, inference)

    session = await sessions.open("user:alice")
    await sessions.append_turn(session.id, Turn(role="user", content="расскажи про память"))
    await memory.remember(
        MemoryItem(text="пользователь интересуется устройством памяти", scope="user:alice")
    )
    await knowledge.add_document(
        "docs",
        Document(text="Память платформы многоуровневая: рабочая, краткосрочная, долговременная.",
                 source="docs/memory.md", trust=TrustLevel.VERIFIED),
    )

    builder = ContextBuilder(sessions, memory, knowledge, FakeTools(), bus)
    bundle = await builder.build(
        ContextRequest(
            intent="как устроена память",
            session_id=session.id,
            scope="user:alice",
            budget_tokens=2000,
        )
    )
    names = [s.name for s in bundle.sections]
    assert {"system", "tools", "knowledge", "memory", "conversation"} <= set(names)
    assert bundle.total_tokens <= bundle.budget_tokens
    knowledge_section = next(s for s in bundle.sections if s.name == "knowledge")
    assert knowledge_section.provenance  # провенанс обязателен
    assert "docs/memory.md" in knowledge_section.content

    await bus.drain()
    assert [e.type for e in await bus.history("context.*")] == ["context.built"]


async def test_context_budget_enforced(inference):
    kv, vs = InMemoryKV(), InMemoryVectorStore()
    knowledge = KnowledgeService(kv, vs, inference)
    await knowledge.add_document(
        "docs", Document(text="очень важное знание " * 500, source="big.md")
    )
    builder = ContextBuilder(knowledge=knowledge)
    bundle = await builder.build(ContextRequest(intent="знание", budget_tokens=100))
    assert bundle.total_tokens <= 100


async def test_context_explicit_include(inference):
    builder = ContextBuilder(system_info="только система")
    bundle = await builder.build(
        ContextRequest(intent="x", include=("system",), budget_tokens=500)
    )
    assert [s.name for s in bundle.sections] == ["system"]
