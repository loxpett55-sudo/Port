"""Contract-тесты портов хранения: гоняются против всех встроенных
реализаций (in-memory и SQLite) — эталон совместимости адаптеров."""

import asyncio

import pytest

from agentos.adapters.memory_store import InMemoryEventLog, InMemoryKV, InMemoryVectorStore
from agentos.adapters.sqlite_store import SqliteEventLog, SqliteKV
from agentos.contracts.storage import VectorItem
from agentos.kernel import Kernel
from agentos.runtimes.event import EventRuntime
from agentos.runtimes.storage import StorageRuntime


@pytest.fixture(params=["memory", "sqlite"])
def kv(request, tmp_path):
    if request.param == "memory":
        return InMemoryKV()
    return SqliteKV(tmp_path / "kv.db")


@pytest.fixture(params=["memory", "sqlite"])
def event_log(request, tmp_path):
    if request.param == "memory":
        return InMemoryEventLog()
    return SqliteEventLog(tmp_path / "log.db")


async def test_kv_contract(kv):
    await kv.put("ns1", "a", {"x": 1})
    await kv.put("ns1", "ab", [1, 2])
    await kv.put("ns2", "a", "другой namespace")

    assert await kv.get("ns1", "a") == {"x": 1}
    assert await kv.get("ns2", "a") == "другой namespace"
    assert sorted(await kv.keys("ns1")) == ["a", "ab"]
    assert await kv.keys("ns1", prefix="ab") == ["ab"]

    assert await kv.delete("ns1", "a") is True
    assert await kv.get("ns1", "a") is None
    assert await kv.delete("ns1", "a") is False


async def test_kv_ttl(kv):
    await kv.put("ns", "temp", "живёт недолго", ttl=0.05)
    assert await kv.get("ns", "temp") == "живёт недолго"
    await asyncio.sleep(0.08)
    assert await kv.get("ns", "temp") is None
    assert await kv.keys("ns") == []


async def test_event_log_contract(event_log):
    o1 = await event_log.append("a.b", {"n": 1})
    o2 = await event_log.append("a.c", {"n": 2})
    assert o2 > o1

    records = await event_log.read()
    assert [r.data["n"] for r in records] == [1, 2]
    tail = await event_log.read(since_offset=o2)
    assert [r.data["n"] for r in tail] == [2]


async def test_vector_store_contract():
    vs = InMemoryVectorStore()
    await vs.upsert(
        "ns",
        [
            VectorItem("cat", [1.0, 0.0], {"kind": "animal"}),
            VectorItem("dog", [0.9, 0.1], {"kind": "animal"}),
            VectorItem("car", [0.0, 1.0], {"kind": "vehicle"}),
        ],
    )
    hits = await vs.search("ns", [1.0, 0.0], k=2)
    assert [h.id for h in hits] == ["cat", "dog"]

    filtered = await vs.search("ns", [1.0, 0.0], k=5, where={"kind": "vehicle"})
    assert [h.id for h in filtered] == ["car"]

    assert await vs.delete("ns", ["cat", "nope"]) == 1


async def test_full_boot_storage_plus_event(tmp_path):
    """Интеграция: ядро поднимает Storage + Event, событие проходит шину."""
    kernel = Kernel({"storage-runtime": {"backend": "sqlite", "path": str(tmp_path)}})
    event_rt = EventRuntime()
    await kernel.boot([event_rt, StorageRuntime()])

    from agentos.contracts.events import Event

    bus = kernel.registry.resolve("EventPort@1")
    got = []

    async def handler(e):
        got.append(e.type)

    bus.subscribe("boot.*", handler)
    await bus.publish(Event(type="boot.test"))
    await bus.drain()
    assert got == ["boot.test"]

    # история durable: пережила бы рестарт (sqlite)
    history = await bus.history("boot.*")
    assert len(history) == 1
    await kernel.shutdown()
