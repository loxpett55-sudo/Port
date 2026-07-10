"""Тесты Event Runtime: подписки, приоритеты, отложенные события, история."""

import asyncio
import time

from agentos.adapters.memory_store import InMemoryEventLog
from agentos.contracts.events import Event, Priority, topic_matches
from agentos.runtimes.event.runtime import EventBus


def test_topic_matching():
    assert topic_matches("task.completed", "task.completed")
    assert topic_matches("task.*", "task.completed")
    assert not topic_matches("task.*", "task.sub.completed")
    assert topic_matches("task.**", "task.sub.completed")
    assert topic_matches("**", "anything.at.all")
    assert not topic_matches("task.failed", "task.completed")


async def make_bus():
    bus = EventBus(InMemoryEventLog())
    await bus.start()
    return bus


async def test_publish_subscribe_wildcard():
    bus = await make_bus()
    got = []

    async def handler(e: Event):
        got.append(e.type)

    bus.subscribe("agent.*", handler)
    await bus.publish(Event(type="agent.spawned"))
    await bus.publish(Event(type="task.completed"))  # не совпадает
    await bus.drain()
    assert got == ["agent.spawned"]
    await bus.stop()


async def test_priority_ordering():
    bus = await make_bus()
    got = []

    async def handler(e: Event):
        got.append(e.payload["n"])

    bus.subscribe("x.*", handler)
    # публикуем вперемешку: background, interactive, normal
    await bus.publish(Event(type="x.a", payload={"n": "bg"}, priority=Priority.BACKGROUND))
    await bus.publish(Event(type="x.b", payload={"n": "int"}, priority=Priority.INTERACTIVE))
    await bus.publish(Event(type="x.c", payload={"n": "norm"}, priority=Priority.NORMAL))
    await bus.drain()
    assert got == ["int", "norm", "bg"]
    await bus.stop()


async def test_consumer_group_competes():
    bus = await make_bus()
    a, b = [], []

    async def worker_a(e: Event):
        a.append(e.id)

    async def worker_b(e: Event):
        b.append(e.id)

    bus.subscribe("job.*", worker_a, durable="workers")
    bus.subscribe("job.*", worker_b, durable="workers")
    for _ in range(10):
        await bus.publish(Event(type="job.run"))
    await bus.drain()
    # каждое событие получил ровно один участник группы
    assert len(a) + len(b) == 10
    assert a and b  # нагрузка распределена
    await bus.stop()


async def test_delayed_event():
    bus = await make_bus()
    got = []

    async def handler(e: Event):
        got.append(time.time())

    bus.subscribe("timer.*", handler)
    t0 = time.time()
    await bus.publish_at(Event(type="timer.fired"), t0 + 0.15)
    await asyncio.sleep(0.05)
    assert got == []  # ещё не время
    await asyncio.sleep(0.25)
    assert len(got) == 1 and got[0] >= t0 + 0.14
    await bus.stop()


async def test_history_and_causation():
    bus = await make_bus()
    cause = Event(type="task.submitted", subject="task:1")
    effect = cause.caused("task.completed", {"ok": True}, subject="task:1")
    await bus.publish(cause)
    await bus.publish(effect)
    await bus.drain()

    history = await bus.history("task.*")
    assert [e.type for e in history] == ["task.submitted", "task.completed"]
    assert history[1].correlation_id == cause.id
    assert history[1].causation_id == cause.id
    await bus.stop()


async def test_failing_handler_does_not_break_bus():
    bus = await make_bus()
    got = []

    async def bad(e: Event):
        raise RuntimeError("boom")

    async def good(e: Event):
        got.append(e.type)

    bus.subscribe("x.*", bad)
    bus.subscribe("x.*", good)
    await bus.publish(Event(type="x.go"))
    await bus.drain()
    assert got == ["x.go"]
    await bus.stop()
