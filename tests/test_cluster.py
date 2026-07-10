"""Тесты Этапа 8: consistent hashing, федерация шин, распределённые агенты."""

import asyncio
from collections import Counter

import pytest

from agentos.adapters.memory_store import InMemoryEventLog
from agentos.adapters.models.stub import StubModel
from agentos.cluster import BusBridge, ConsistentHashRing, DistributedAgentGateway
from agentos.contracts.agent import AgentDefinition
from agentos.contracts.errors import AgentOSError
from agentos.contracts.events import Event
from agentos.runtimes.agent import AgentService
from agentos.runtimes.event.runtime import EventBus
from agentos.runtimes.inference import InferenceEngine
from agentos.runtimes.model import ModelRegistry


# --- Hash ring ---------------------------------------------------------------

def test_ring_distribution_and_minimal_migration():
    ring = ConsistentHashRing()
    for n in ("node-a", "node-b", "node-c"):
        ring.add_node(n)

    keys = [f"agent-{i}/session-{i}" for i in range(3000)]
    before = {k: ring.owner(k) for k in keys}
    counts = Counter(before.values())
    assert set(counts) == {"node-a", "node-b", "node-c"}
    assert min(counts.values()) > 600  # распределение примерно равномерно

    ring.remove_node("node-b")
    after = {k: ring.owner(k) for k in keys}
    moved = [k for k in keys if before[k] != after[k]]
    # мигрировали только ключи погибшего узла
    assert all(before[k] == "node-b" for k in moved)
    assert not any(after[k] == "node-b" for k in keys)


def test_ring_empty():
    with pytest.raises(LookupError):
        ConsistentHashRing().owner("x")


# --- Bus federation ------------------------------------------------------------

async def make_node():
    bus = EventBus(InMemoryEventLog())
    await bus.start()
    return bus


async def test_bridge_forwards_without_echo():
    bus_a, bus_b = await make_node(), await make_node()
    bridge_a = BusBridge(bus_a, "node-a")
    bridge_b = BusBridge(bus_b, "node-b")
    await bridge_a.start()
    await bridge_b.start()
    await bridge_b.connect("127.0.0.1", bridge_a.port)

    got_a, got_b = [], []

    async def on_a(e):
        got_a.append(e.id)

    async def on_b(e):
        got_b.append(e.id)

    bus_a.subscribe("test.*", on_a)
    bus_b.subscribe("test.*", on_b)

    event = Event(type="test.ping", payload={"n": 1})
    await bus_a.publish(event)
    await asyncio.sleep(0.15)  # доставка по TCP

    assert got_a == [event.id]
    assert got_b == [event.id]      # дошло на второй узел
    await asyncio.sleep(0.1)
    assert got_a == [event.id]      # эха обратно не было

    # обратное направление
    event2 = Event(type="test.pong")
    await bus_b.publish(event2)
    await asyncio.sleep(0.15)
    assert event2.id in got_a

    for br in (bridge_a, bridge_b):
        await br.stop()
    for bus in (bus_a, bus_b):
        await bus.stop()


# --- Distributed agents ------------------------------------------------------------

def make_agents(bus, reply_text):
    registry = ModelRegistry()
    registry.register(StubModel([reply_text]))
    return AgentService(inference=InferenceEngine(registry), events=bus)


async def test_distributed_agent_request_reply():
    """Узел A (без агентов) отправляет сообщение агенту на узле B."""
    bus_a, bus_b = await make_node(), await make_node()
    bridge_a, bridge_b = BusBridge(bus_a, "node-a"), BusBridge(bus_b, "node-b")
    await bridge_a.start()
    await bridge_b.start()
    await bridge_b.connect("127.0.0.1", bridge_a.port)

    ring = ConsistentHashRing()
    ring.add_node("node-b")  # все агенты живут на B

    agents_b = make_agents(bus_b, "ответ с узла B")
    agents_b.register(AgentDefinition(id="remote-worker"))

    gw_a = DistributedAgentGateway(bus_a, ring, "node-a", local_agents=None)
    gw_b = DistributedAgentGateway(bus_b, ring, "node-b", local_agents=agents_b)
    gw_a.start()
    gw_b.start()

    reply = await gw_a.send("remote-worker", "привет через кластер", timeout=5)
    assert reply.text == "ответ с узла B"

    # ошибка удалённого узла доходит до вызывающего
    with pytest.raises(AgentOSError):
        await gw_a.send("ghost-agent", "нет такого", timeout=5)

    await agents_b.stop_all()
    for br in (bridge_a, bridge_b):
        await br.stop()
    for bus in (bus_a, bus_b):
        await bus.stop()


async def test_local_owner_short_circuit():
    """Если владелец — локальный узел, шина не используется."""
    bus = await make_node()
    ring = ConsistentHashRing()
    ring.add_node("solo")
    agents = make_agents(bus, "локальный ответ")
    agents.register(AgentDefinition(id="local-agent"))
    gw = DistributedAgentGateway(bus, ring, "solo", local_agents=agents)
    gw.start()
    reply = await gw.send("local-agent", "привет")
    assert reply.text == "локальный ответ"
    await agents.stop_all()
    await bus.stop()
