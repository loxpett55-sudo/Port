"""Тесты Этапа 7: composition root, API Runtime, SDK, Dashboard, метрики.

E2E: полная платформа поднимается ядром, запросы идут через настоящий
HTTP (SDK-клиент в пуле потоков) до агента со StubModel и обратно.
"""

import asyncio

import pytest

from agentos.adapters.models.stub import HashEmbedder, StubModel
from agentos.contracts.agent import AgentDefinition
from agentos.contracts.model import ToolCall
from agentos.contracts.tool import ToolManifest
from agentos.platform import AgentOSPlatform
from agentos.runtimes.api import ApiRuntime
from agentos.sdk import AgentOSClient


@pytest.fixture
async def platform():
    p = AgentOSPlatform(
        models=[
            StubModel(
                [
                    ToolCall(id="c1", name="calc", arguments={"a": 6, "b": 7}),
                    "Ответ: 42",
                    "обычный ответ",
                ]
            ),
            HashEmbedder(),
        ]
    )
    api = ApiRuntime(p, port=0)  # порт 0 — свободный
    p._modules.append(api)
    await p.start()

    async def calc(args, ctx):
        return {"result": args["a"] * args["b"]}

    p.tools.register(
        ToolManifest(
            id="calc", version="1.0.0", description="Умножение",
            input_schema={"type": "object",
                          "properties": {"a": {"type": "integer"}, "b": {"type": "integer"}},
                          "required": ["a", "b"]},
        ),
        calc,
    )
    p.agents.register(AgentDefinition(id="assistant", tools=("calc",)))
    p.api_port = api.server.port
    yield p
    await p.stop()


@pytest.fixture
def client(platform):
    return AgentOSClient(f"http://127.0.0.1:{platform.api_port}")


async def call(fn, *args, **kw):
    return await asyncio.to_thread(fn, *args, **kw)


async def test_platform_boots_all_modules(platform):
    health = platform.health()
    assert len(health) >= 17
    assert all(v["state"] == "ok" for v in health.values())


async def test_api_health_and_models(client):
    health = await call(client.health)
    assert health["status"] == "ok"
    models = await call(client.models)
    assert {m["id"] for m in models} == {"stub-chat", "stub-embed"}


async def test_api_agent_message_e2e(client):
    session_id = await call(client.open_session, "user:test")
    reply = await call(client.send, "assistant", "сколько будет 6×7?",
                       session_id=session_id)
    assert reply["text"] == "Ответ: 42"
    assert reply["tool_calls"] == 1

    # ход записан в сессию
    turns = await call(client.request, "GET", f"/v1/sessions/{session_id}/turns")
    roles = [t["role"] for t in turns["turns"]]
    assert roles == ["user", "assistant"]


async def test_api_knowledge_roundtrip(client):
    result = await call(
        client.add_document, "docs",
        "Платформа AgentOS построена на микроядре и событийной шине.",
        source="test.md", trust="verified",
    )
    assert result["chunks"] == 1
    hits = await call(client.search, "на чём построена платформа")
    assert hits and hits[0]["source"] == "test.md"


async def test_api_register_agent_and_errors(client):
    agent_id = await call(client.register_agent, "writer",
                          personality="Технический писатель")
    assert agent_id == "writer"
    agents = await call(client.agents)
    assert "writer" in {a["id"] for a in agents}

    from agentos.contracts.errors import AgentOSError

    with pytest.raises(AgentOSError, match="не зарегистрирован"):
        await call(client.send, "ghost", "привет")


async def test_api_metrics_and_events(client):
    await call(client.send, "assistant", "ещё вопрос")
    metrics = await call(client.metrics)
    assert metrics["events"].get("inference.completed", 0) >= 1
    assert "stub-chat" in metrics["inference"]["tokens_by_model"]

    events = await call(client.events, "agent.**")
    assert any(e["type"] == "agent.turn.completed" for e in events)


async def test_dashboard_served(client):
    import urllib.request

    def fetch():
        base = client._base
        with urllib.request.urlopen(f"{base}/") as resp:
            return resp.status, resp.read().decode()

    status, html = await call(fetch)
    assert status == 200 and "Runtime Dashboard" in html


async def test_api_unknown_route(client):
    from agentos.contracts.errors import AgentOSError

    with pytest.raises(AgentOSError, match="нет маршрута"):
        await call(client.request, "GET", "/v1/nonexistent")
