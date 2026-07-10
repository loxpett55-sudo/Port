"""Тесты Этапа 6: Policy, Security, Resource."""

import asyncio

import pytest

from agentos.adapters.memory_store import InMemoryEventLog, InMemoryKV
from agentos.contracts.errors import AgentOSError, PermissionDeniedError, ResourceExhaustedError
from agentos.contracts.events import Event
from agentos.contracts.policy import PolicyRule
from agentos.contracts.resource import Budget
from agentos.contracts.security import PermissionSet, capability_matches
from agentos.contracts.tool import ToolCallContext, ToolManifest
from agentos.runtimes.event.runtime import EventBus
from agentos.runtimes.policy import PolicyService
from agentos.runtimes.resource import ResourceService
from agentos.runtimes.security import AuditLog, SecretsVault, ThreatDetector
from agentos.runtimes.tool import ToolRegistry


@pytest.fixture
async def bus():
    b = EventBus(InMemoryEventLog())
    await b.start()
    yield b
    await b.stop()


# --- Policy -------------------------------------------------------------------

async def test_policy_priority_and_deny_wins():
    policy = PolicyService()
    policy.add_rule(PolicyRule(id="allow-all-tools", action="tool.*", effect="allow", priority=0))
    policy.add_rule(
        PolicyRule(
            id="deny-shell",
            action="tool.invoke",
            effect="deny",
            resource={"tool_id": "shell"},
            priority=10,
            message="shell запрещён",
        )
    )
    ok = await policy.evaluate(action="tool.invoke", resource={"tool_id": "calc"})
    assert ok.allowed and ok.rule_id == "allow-all-tools"

    denied = await policy.evaluate(action="tool.invoke", resource={"tool_id": "shell"})
    assert not denied.allowed and denied.reason == "shell запрещён"


async def test_policy_condition_and_subject_match():
    policy = PolicyService(default_effect="deny")
    policy.add_rule(
        PolicyRule(
            id="pii-local-only",
            action="inference.generate",
            effect="allow",
            resource={"locality": "local"},
            condition=lambda env: "pii" in env.get("data_classes", []),
            priority=5,
        )
    )
    allowed = await policy.evaluate(
        action="inference.generate",
        resource={"locality": "local"},
        context={"data_classes": ["pii"]},
    )
    assert allowed.allowed

    cloud = await policy.evaluate(
        action="inference.generate",
        resource={"locality": "cloud"},
        context={"data_classes": ["pii"]},
    )
    assert not cloud.allowed and cloud.reason == "default-deny"


async def test_policy_obligations_and_list_matcher():
    policy = PolicyService()
    policy.add_rule(
        PolicyRule(
            id="confirm-destructive",
            action="tool.invoke",
            effect="allow",
            resource={"tool_id": ["delete", "deploy"]},
            obligations=("confirm-human", "audit"),
            priority=1,
        )
    )
    d = await policy.evaluate(action="tool.invoke", resource={"tool_id": "deploy"})
    assert d.allowed and "confirm-human" in d.obligations


async def test_policy_integrated_with_tools(bus):
    policy = PolicyService(bus)
    policy.add_rule(
        PolicyRule(
            id="deny-evil-agent",
            action="tool.invoke",
            effect="deny",
            subject={"agent_id": "evil"},
            priority=10,
            message="агент заблокирован",
        )
    )
    tools = ToolRegistry(events=bus, policy=policy)
    tools.register(
        ToolManifest(id="echo", version="1", description="",
                     input_schema={"type": "object"}),
        lambda args, ctx: _now(args),
    )
    ok = await tools.invoke("echo", {}, ToolCallContext(agent_id="good"))
    assert ok.ok
    with pytest.raises(PermissionDeniedError, match="заблокирован"):
        await tools.invoke("echo", {}, ToolCallContext(agent_id="evil"))


async def _now(x):
    return x


# --- Security -----------------------------------------------------------------

def test_capability_matching():
    assert capability_matches("*", "anything:at:all")
    assert capability_matches("storage:*", "storage:kv:read")
    assert not capability_matches("storage:kv", "storage:kv:read")
    assert capability_matches("storage:kv:read", "storage:kv:read")
    assert not capability_matches("network:*", "storage:kv:read")

    perms = PermissionSet(frozenset({"storage:*", "network:egress:https"}))
    assert perms.allows("storage:vector:write")
    assert perms.allows("network:egress:https")
    assert not perms.allows("network:egress:http")


async def test_secrets_roundtrip_lease_and_audit():
    audit = AuditLog()
    vault = SecretsVault(InMemoryKV(), audit)
    await vault.put("api-key", "sk-очень-секретно")
    lease = await vault.lease("api-key", ttl=60, actor="tool:web")
    assert lease.value == "sk-очень-секретно" and lease.alive

    with pytest.raises(AgentOSError, match="не найден"):
        await vault.lease("nope")
    assert await vault.delete("api-key") is True

    records = await audit.read()
    actions = [r.action for r in records]
    assert actions == ["secret.put", "secret.lease", "secret.delete"]
    # значение секрета не утекло в аудит
    assert all("сек" not in str(r.detail.values()) for r in records)


async def test_secret_encrypted_at_rest():
    kv = InMemoryKV()
    vault = SecretsVault(kv, None)
    await vault.put("k", "plaintext-value")
    stored = await kv.get("secrets", "k")
    assert "plaintext-value" not in str(stored)


async def test_audit_chain_verify_and_tamper():
    audit = AuditLog()
    for i in range(5):
        await audit.record("actor", f"action-{i}", {"i": i})
    assert await audit.verify() is True

    # подмена записи ломает цепочку
    records = await audit.read()
    object.__setattr__(records[2], "detail", {"i": 999})
    assert await audit.verify() is False


async def test_threat_detector(bus):
    detector = ThreatDetector(bus, window_seconds=5, deny_threshold=3)
    detector.start()
    for _ in range(3):
        await bus.publish(
            Event(type="tool.denied", payload={"agent_id": "suspicious"})
        )
    await bus.drain()
    threats = await bus.history("security.threat.detected")
    assert len(threats) == 1
    assert threats[0].payload["actor"] == "suspicious"


# --- Resource ------------------------------------------------------------------

async def test_resource_budget_enforcement(bus):
    res = ResourceService(bus)
    res.set_budget("session:s1", Budget(max_tokens=1000))
    res.check("session:s1")  # пусто — ок

    await res._consume("session:s1", tokens=900)
    res.check("session:s1")  # ещё в пределах
    await res._consume("session:s1", tokens=200)
    with pytest.raises(ResourceExhaustedError, match="токенов"):
        res.check("session:s1")

    await bus.drain()
    warnings = await bus.history("budget.threshold.crossed")
    assert len(warnings) == 1  # предупреждение один раз


async def test_resource_passive_accounting(bus):
    res = ResourceService()
    res.attach(bus)
    await bus.publish(
        Event(
            type="inference.completed",
            payload={"prompt_tokens": 100, "completion_tokens": 50, "cost": 0.01},
            subject="session:s2",
        )
    )
    await bus.publish(Event(type="tool.completed", payload={}, subject="session:s2"))
    await bus.drain()
    usage = res.usage("session:s2")
    assert usage.tokens == 150 and usage.cost == 0.01 and usage.tool_calls == 1


async def test_context_budget_degradation():
    res = ResourceService()
    res.set_budget("agent:a", Budget(max_tokens=500))
    await res._consume("agent:a", tokens=400)
    # осталось 100 — просим 4000, получаем деградированный бюджет
    assert res.context_budget("agent:a", 4000) == 100
    assert res.context_budget("agent:no-budget", 4000) == 4000
