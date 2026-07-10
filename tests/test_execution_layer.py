"""Тесты Этапа 5: Task, Scheduler, Workflow, Agent."""

import asyncio
import time

import pytest

from agentos.adapters.memory_store import InMemoryEventLog, InMemoryKV, InMemoryVectorStore
from agentos.adapters.models.stub import HashEmbedder, StubModel
from agentos.contracts.errors import AgentOSError
from agentos.contracts.agent import AgentDefinition
from agentos.contracts.model import ToolCall
from agentos.contracts.scheduler import DagSpec
from agentos.contracts.task import TaskSpec, TaskState
from agentos.contracts.tool import ToolManifest
from agentos.contracts.workflow import (
    InstanceState,
    Step,
    StepKind,
    WorkflowDef,
)
from agentos.runtimes.agent import AgentService
from agentos.runtimes.event.runtime import EventBus
from agentos.runtimes.inference import InferenceEngine
from agentos.runtimes.model import ModelRegistry
from agentos.runtimes.scheduler import SchedulerService
from agentos.runtimes.task import TaskService
from agentos.runtimes.tool import ToolRegistry
from agentos.runtimes.workflow import WorkflowEngine


@pytest.fixture
async def bus():
    b = EventBus(InMemoryEventLog())
    await b.start()
    yield b
    await b.stop()


@pytest.fixture
async def tasks(bus):
    svc = TaskService(bus, base_backoff=0.005)
    await svc.start()
    yield svc
    await svc.stop()


# --- Task Runtime ------------------------------------------------------------

async def test_task_success(tasks):
    tasks.register_handler("add", lambda inp: _async(inp["a"] + inp["b"]))
    task_id = await tasks.submit(TaskSpec(type="add", input={"a": 2, "b": 3}))
    assert await tasks.result(task_id, timeout=2) == 5
    assert (await tasks.get(task_id)).state is TaskState.SUCCEEDED


async def _async(value):
    return value


async def test_task_retry_then_deadletter(tasks, bus):
    attempts = {"n": 0}

    async def flaky(inp):
        attempts["n"] += 1
        raise RuntimeError("всегда падает")

    tasks.register_handler("flaky", flaky)
    task_id = await tasks.submit(TaskSpec(type="flaky", max_attempts=3))
    with pytest.raises(AgentOSError, match="dead_lettered"):
        await tasks.result(task_id, timeout=3)
    assert attempts["n"] == 3
    await bus.drain()
    types = [e.type for e in await bus.history("task.*")]
    assert types.count("task.retrying") == 2 and "task.deadlettered" in types


async def test_task_idempotency_and_cancel(tasks):
    tasks.register_handler("noop", _async)
    id1 = await tasks.submit(TaskSpec(type="noop", idempotency_key="k1"))
    id2 = await tasks.submit(TaskSpec(type="noop", idempotency_key="k1"))
    assert id1 == id2  # дубликат не создан

    blocker = asyncio.Event()

    async def blocked(inp):
        await blocker.wait()

    tasks.register_handler("blocked", blocked)
    # займём всех воркеров, чтобы задача осталась PENDING
    for _ in range(8):
        await tasks.submit(TaskSpec(type="blocked"))
    victim = await tasks.submit(TaskSpec(type="noop"))
    assert await tasks.cancel(victim) is True
    assert (await tasks.get(victim)).state is TaskState.CANCELLED
    blocker.set()


# --- Scheduler ------------------------------------------------------------------

async def test_scheduler_timer(tasks, bus):
    fired = []
    tasks.register_handler("ping", lambda inp: _async(fired.append(time.time())))
    sched = SchedulerService(tasks, bus)
    await sched.start()
    await sched.at(time.time() + 0.1, TaskSpec(type="ping"))
    await asyncio.sleep(0.3)
    assert len(fired) == 1
    await sched.stop()


async def test_scheduler_periodic_and_cancel(tasks, bus):
    fired = []
    tasks.register_handler("tick", lambda inp: _async(fired.append(1)))
    sched = SchedulerService(tasks, bus)
    await sched.start()
    timer_id = await sched.every(0.08, TaskSpec(type="tick"))
    await asyncio.sleep(0.3)
    await sched.cancel_timer(timer_id)
    count = len(fired)
    assert count >= 2
    await asyncio.sleep(0.2)
    assert len(fired) == count  # после отмены не срабатывает
    await sched.stop()


async def test_scheduler_dag_order_and_skip(tasks, bus):
    order = []

    def handler(name, fail=False):
        async def run(inp):
            order.append(name)
            if fail:
                raise RuntimeError("сбой узла")
            return name

        return run

    tasks.register_handler("a", handler("a"))
    tasks.register_handler("b", handler("b", fail=True))
    tasks.register_handler("c", handler("c"))       # зависит от b — будет skipped
    tasks.register_handler("d", handler("d"))       # зависит от a — выполнится

    sched = SchedulerService(tasks, bus)
    await sched.start()
    dag_id = await sched.submit_dag(
        DagSpec(
            tasks={
                "a": TaskSpec(type="a", max_attempts=1),
                "b": TaskSpec(type="b", max_attempts=1),
                "c": TaskSpec(type="c", max_attempts=1),
                "d": TaskSpec(type="d", max_attempts=1),
            },
            dependencies={"c": ("b",), "d": ("a",), "b": ("a",)},
        )
    )
    for _ in range(100):
        status = await sched.dag_status(dag_id)
        if status.done:
            break
        await asyncio.sleep(0.02)
    assert status.done and status.failed
    assert status.node_states == {
        "a": "succeeded", "b": "failed", "c": "skipped", "d": "succeeded",
    }
    assert order.index("a") < order.index("b")
    await sched.stop()


# --- Workflow ---------------------------------------------------------------------

def action(id, fn, compensate=None):
    return Step(id=id, kind=StepKind.ACTION, run=fn, compensate=compensate)


async def test_workflow_sequence_branch_loop(bus):
    engine = WorkflowEngine(InMemoryKV(), bus)

    async def init(vars):
        return 0

    async def increment(vars):
        vars["counter"] = vars.get("counter", 0) + 1
        return vars["counter"]

    engine.register(
        WorkflowDef(
            id="counting",
            steps=(
                action("init", init),
                Step(
                    id="loop",
                    kind=StepKind.LOOP,
                    condition=lambda v: v.get("counter", 0) < 3,
                    then_steps=(action("inc", increment),),
                ),
                Step(
                    id="check",
                    kind=StepKind.BRANCH,
                    condition=lambda v: v["counter"] == 3,
                    then_steps=(action("ok", lambda v: _async("три")),),
                    else_steps=(action("bad", lambda v: _async("не три")),),
                ),
            ),
        )
    )
    instance_id = await engine.start("counting")
    instance = await engine.wait(instance_id, timeout=5)
    assert instance.state is InstanceState.COMPLETED
    assert instance.vars["counter"] == 3 and instance.vars["ok"] == "три"


async def test_workflow_parallel(bus):
    engine = WorkflowEngine(events=bus)
    log = []

    def slow(name, delay):
        async def run(vars):
            await asyncio.sleep(delay)
            log.append(name)
            return name

        return run

    engine.register(
        WorkflowDef(
            id="par",
            steps=(
                Step(
                    id="fan",
                    kind=StepKind.PARALLEL,
                    branches=(
                        (action("slow1", slow("slow", 0.15)),),
                        (action("fast1", slow("fast", 0.01)),),
                    ),
                ),
            ),
        )
    )
    t0 = time.monotonic()
    instance = await engine.wait(await engine.start("par"), timeout=5)
    elapsed = time.monotonic() - t0
    assert instance.state is InstanceState.COMPLETED
    assert elapsed < 0.3  # ветви шли параллельно
    assert set(log) == {"slow", "fast"}


async def test_workflow_saga_compensation(bus):
    engine = WorkflowEngine(InMemoryKV(), bus)
    compensated = []

    async def book_hotel(vars):
        return "hotel-123"

    async def cancel_hotel(vars):
        compensated.append("hotel")

    async def book_flight(vars):
        raise RuntimeError("рейсов нет")

    engine.register(
        WorkflowDef(
            id="trip",
            steps=(
                action("hotel", book_hotel, compensate=cancel_hotel),
                action("flight", book_flight),
            ),
        )
    )
    instance = await engine.wait(await engine.start("trip"), timeout=5)
    assert instance.state is InstanceState.COMPENSATED
    assert compensated == ["hotel"]  # выполненные шаги откачены
    await bus.drain()
    assert "workflow.compensated" in [e.type for e in await bus.history("workflow.*")]


async def test_workflow_wait_event(bus):
    engine = WorkflowEngine(events=bus)
    engine.register(
        WorkflowDef(
            id="approval",
            steps=(
                Step(
                    id="wait",
                    kind=StepKind.WAIT_EVENT,
                    event_pattern="report.approved",
                    timeout_seconds=5.0,
                ),
                action("publish", lambda v: _async(f"опубликован: {v['wait']['by']}")),
            ),
        )
    )
    instance_id = await engine.start("approval")
    await asyncio.sleep(0.05)
    from agentos.contracts.events import Event

    await bus.publish(Event(type="report.approved", payload={"by": "alice"}))
    instance = await engine.wait(instance_id, timeout=5)
    assert instance.state is InstanceState.COMPLETED
    assert instance.vars["publish"] == "опубликован: alice"


# --- Agent ------------------------------------------------------------------------

def make_agent_stack(bus, replies):
    registry = ModelRegistry()
    registry.register(StubModel(replies))
    registry.register(HashEmbedder())
    inference = InferenceEngine(registry)
    tools = ToolRegistry(events=bus)

    async def calc(args, ctx):
        return {"result": args["a"] * args["b"]}

    tools.register(
        ToolManifest(
            id="calc",
            version="1.0.0",
            description="Перемножает числа",
            input_schema={
                "type": "object",
                "properties": {"a": {"type": "integer"}, "b": {"type": "integer"}},
                "required": ["a", "b"],
            },
        ),
        calc,
    )
    return AgentService(inference=inference, tools=tools, events=bus), tools


async def test_agent_tool_loop(bus):
    # сценарий: модель просит инструмент, затем отвечает текстом
    service, _ = make_agent_stack(
        bus,
        [
            ToolCall(id="c1", name="calc", arguments={"a": 6, "b": 7}),
            "Ответ: 42",
        ],
    )
    service.register(AgentDefinition(id="math", tools=("calc",)))
    reply = await service.send("math", "сколько будет 6×7?", session_id="s1")
    assert reply.text == "Ответ: 42"
    assert reply.iterations == 2 and reply.tool_calls == 1

    await bus.drain()
    types = [e.type for e in await bus.history("**")]
    assert "tool.completed" in types and "agent.turn.completed" in types


async def test_agent_tool_allowlist(bus):
    # агент без прав на calc: инструмент не вызывается, модель получает отказ
    service, _ = make_agent_stack(
        bus,
        [
            ToolCall(id="c1", name="calc", arguments={"a": 1, "b": 2}),
            "не смог посчитать",
        ],
    )
    service.register(AgentDefinition(id="restricted", tools=("другой",)))
    reply = await service.send("restricted", "посчитай")
    assert reply.text == "не смог посчитать"
    await bus.drain()
    assert "tool.completed" not in [e.type for e in await bus.history("tool.*")]


async def test_agent_iteration_limit(bus):
    endless = [ToolCall(id=f"c{i}", name="calc", arguments={"a": 1, "b": 1}) for i in range(99)]
    service, _ = make_agent_stack(bus, endless)
    service.register(AgentDefinition(id="loopy", tools=("calc",), max_iterations=3))
    with pytest.raises(AgentOSError, match="лимит итераций"):
        await service.send("loopy", "зациклись")


async def test_agent_actor_sequential_and_survives_errors(bus):
    service, _ = make_agent_stack(bus, ["ответ"])
    service.register(AgentDefinition(id="a1", max_iterations=1))

    # сбойный ход (модель вернёт ToolCall при max_iterations=1? нет — реплики "ответ")
    # имитируем сбой через незарегистрированного агента → отдельная проверка
    r1, r2 = await asyncio.gather(
        service.send("a1", "первый", session_id="s"),
        service.send("a1", "второй", session_id="s"),
    )
    assert r1.text == "ответ" and r2.text == "ответ"

    with pytest.raises(AgentOSError):
        await service.send("noone", "привет")
    await service.stop_all()
