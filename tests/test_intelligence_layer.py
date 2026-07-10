"""Тесты Этапа 3: Model Registry, Inference Engine, Tool Runtime."""

import asyncio

import pytest

from agentos.adapters.memory_store import InMemoryEventLog, InMemoryKV
from agentos.adapters.models.stub import HashEmbedder, StubModel, user
from agentos.contracts.errors import PermissionDeniedError, PortNotFoundError, TransientError
from agentos.contracts.inference import InferenceRequest
from agentos.contracts.model import (
    CAP_EMBED,
    CAP_TEXT,
    GenerateRequest,
    ModelRequirements,
    ToolCall,
)
from agentos.contracts.tool import ToolCallContext, ToolManifest
from agentos.lib.jsonschema import validate
from agentos.runtimes.event.runtime import EventBus
from agentos.runtimes.inference import InferenceEngine
from agentos.runtimes.model import ModelRegistry
from agentos.runtimes.tool import ToolRegistry


# --- Model Registry -------------------------------------------------------

def test_select_by_requirements():
    reg = ModelRegistry()
    cheap = StubModel(model_id="cheap", cost_in=0.1, cost_out=0.1, context_window=8000)
    big = StubModel(model_id="big", cost_in=5.0, cost_out=15.0, context_window=200_000)
    reg.register(cheap)
    reg.register(big, aliases=["default-chat"])
    reg.register(HashEmbedder())

    assert reg.select(ModelRequirements()).descriptor().id == "cheap"
    assert (
        reg.select(ModelRequirements(prefer="largest-context")).descriptor().id == "big"
    )
    assert reg.select(ModelRequirements(min_context=100_000)).descriptor().id == "big"
    assert reg.select(ModelRequirements(capabilities=frozenset({CAP_EMBED}))).descriptor().id == "stub-embed"
    assert reg.get("default-chat").descriptor().id == "big"

    with pytest.raises(PortNotFoundError):
        reg.select(ModelRequirements(min_context=1_000_000))


# --- Inference ------------------------------------------------------------

def make_engine(model, **kw):
    reg = ModelRegistry()
    reg.register(model)
    return InferenceEngine(reg, kw.pop("kv", None), kw.pop("events", None), **kw)


async def test_generate_collects_stream():
    engine = make_engine(StubModel(["привет мир"]))
    result = await engine.generate(
        InferenceRequest(request=GenerateRequest(messages=(user("hi"),)))
    )
    assert result.text == "привет мир"
    assert result.finish_reason == "stop"
    assert result.usage.prompt_tokens > 0


async def test_streaming_chunks():
    engine = make_engine(StubModel(["один два три"]))
    chunks = []
    async for c in engine.stream(
        InferenceRequest(request=GenerateRequest(messages=(user("hi"),)))
    ):
        chunks.append(c)
    assert len(chunks) == 3  # по словам
    assert "".join(c.delta for c in chunks) == "один два три"
    assert chunks[-1].usage is not None


async def test_retry_on_transient_error():
    model = StubModel(["готово"], fail_times=2)
    engine = make_engine(model, base_backoff=0.001)
    result = await engine.generate(
        InferenceRequest(request=GenerateRequest(messages=(user("x"),)))
    )
    assert result.text == "готово"
    assert model.calls == 3  # 2 сбоя + успех


async def test_retries_exhausted():
    model = StubModel(["никогда"], fail_times=10)
    engine = make_engine(model, base_backoff=0.001)
    with pytest.raises(TransientError):
        await engine.generate(
            InferenceRequest(request=GenerateRequest(messages=(user("x"),)), max_attempts=2)
        )
    assert model.calls == 2


async def test_cache_for_deterministic_requests():
    model = StubModel(["кэшируемый ответ"])
    engine = make_engine(model, kv=InMemoryKV())
    req = InferenceRequest(
        request=GenerateRequest(messages=(user("q"),), temperature=0.0)
    )
    r1 = await engine.generate(req)
    r2 = await engine.generate(req)
    assert r1.text == r2.text == "кэшируемый ответ"
    assert model.calls == 1  # второй раз — из кэша

    # temperature > 0 не кэшируется
    hot = InferenceRequest(request=GenerateRequest(messages=(user("q"),), temperature=0.7))
    await engine.generate(hot)
    await engine.generate(hot)
    assert model.calls == 3


async def test_usage_events_published():
    bus = EventBus(InMemoryEventLog())
    await bus.start()
    engine = make_engine(StubModel(["ответ"]), events=bus)
    await engine.generate(
        InferenceRequest(request=GenerateRequest(messages=(user("hi"),)), session_id="s1")
    )
    await bus.drain()
    history = await bus.history("inference.*")
    assert len(history) == 1
    assert history[0].payload["prompt_tokens"] > 0
    await bus.stop()


async def test_embed_batching():
    engine = make_engine(HashEmbedder(), embed_batch_size=2)
    vectors = await engine.embed(
        ["a", "b", "c", "d", "e"], ModelRequirements(capabilities=frozenset({CAP_EMBED}))
    )
    assert len(vectors) == 5
    # тексты с общими словами ближе
    v_cat1, v_cat2, v_car = await engine.embed(
        ["кот сидит на крыше", "кот спит на крыше", "автомобиль едет по трассе"],
        ModelRequirements(capabilities=frozenset({CAP_EMBED})),
    )
    from agentos.adapters.memory_store import cosine

    assert cosine(v_cat1, v_cat2) > cosine(v_cat1, v_car)


async def test_priority_gate_orders_waiters():
    from agentos.runtimes.inference.runtime import PriorityGate

    gate = PriorityGate(1)
    await gate.acquire(1)
    order = []

    async def worker(prio, name):
        await gate.acquire(prio)
        order.append(name)
        gate.release()

    tasks = [
        asyncio.create_task(worker(2, "bg")),
        asyncio.create_task(worker(0, "int")),
        asyncio.create_task(worker(1, "norm")),
    ]
    await asyncio.sleep(0.01)  # все встали в очередь
    gate.release()
    await asyncio.gather(*tasks)
    assert order == ["int", "norm", "bg"]


# --- Tools ------------------------------------------------------------------

ECHO_MANIFEST = ToolManifest(
    id="echo",
    version="1.0.0",
    description="Возвращает переданный текст",
    input_schema={
        "type": "object",
        "properties": {"text": {"type": "string"}},
        "required": ["text"],
    },
    output_schema={"type": "object", "properties": {"echo": {"type": "string"}}},
    timeout_seconds=0.5,
)


async def echo_handler(args, ctx):
    return {"echo": args["text"]}


async def test_tool_invoke_success():
    tools = ToolRegistry()
    tools.register(ECHO_MANIFEST, echo_handler)
    result = await tools.invoke("echo", {"text": "привет"}, ToolCallContext(agent_id="a1"))
    assert result.ok and result.output == {"echo": "привет"}
    assert tools.schemas()[0].name == "echo"


async def test_tool_input_validation():
    tools = ToolRegistry()
    tools.register(ECHO_MANIFEST, echo_handler)
    result = await tools.invoke("echo", {"wrong": 1}, ToolCallContext())
    assert not result.ok and "text" in result.error


async def test_tool_timeout():
    async def slow(args, ctx):
        await asyncio.sleep(1.0)

    tools = ToolRegistry()
    tools.register(ECHO_MANIFEST, slow)
    result = await tools.invoke("echo", {"text": "x"}, ToolCallContext())
    assert not result.ok and "тайм-аут" in result.error


async def test_tool_policy_denial_and_audit():
    class DenyPolicy:
        async def evaluate(self, **kw):
            class D:
                allowed = False
                reason = "запрещено политикой теста"

            return D()

    bus = EventBus(InMemoryEventLog())
    await bus.start()
    tools = ToolRegistry(events=bus, policy=DenyPolicy())
    tools.register(ECHO_MANIFEST, echo_handler)
    with pytest.raises(PermissionDeniedError):
        await tools.invoke("echo", {"text": "x"}, ToolCallContext(agent_id="evil"))
    await bus.drain()
    audit = await bus.history("tool.denied")
    assert len(audit) == 1 and audit[0].payload["agent_id"] == "evil"
    await bus.stop()


# --- JSON Schema валидатор ---------------------------------------------------

def test_jsonschema_subset():
    schema = {
        "type": "object",
        "properties": {
            "n": {"type": "integer", "minimum": 0, "maximum": 10},
            "tags": {"type": "array", "items": {"type": "string"}},
            "mode": {"type": "string", "enum": ["a", "b"]},
        },
        "required": ["n"],
        "additionalProperties": False,
    }
    assert validate(schema, {"n": 5, "tags": ["x"], "mode": "a"}) == []
    assert validate(schema, {}) != []            # нет required
    assert validate(schema, {"n": 99}) != []     # maximum
    assert validate(schema, {"n": 1, "mode": "c"}) != []  # enum
    assert validate(schema, {"n": 1, "extra": 1}) != []   # additionalProperties
    assert validate(schema, {"n": True}) != []   # bool не integer
