"""Policy Runtime — Policy Decision Point.

Правила декларативны, оцениваются по приоритету; при равенстве
приоритета deny побеждает allow. Решения публикуются в аудит событием
policy.decision (obligation "audit")."""

from __future__ import annotations

from typing import Any

from agentos.contracts.events import Event, EventPort, topic_matches
from agentos.contracts.module import ModuleContext, ModuleManifest, RuntimeModule
from agentos.contracts.policy import Decision, PolicyPort, PolicyRule


def _matches(matcher: dict[str, Any], attrs: dict[str, Any]) -> bool:
    """Matcher совпадает, если каждый его атрибут равен ('*' — любой).
    Значение-список в matcher означает «одно из»."""
    for key, expected in matcher.items():
        actual = attrs.get(key)
        if expected == "*":
            if key not in attrs:
                return False
        elif isinstance(expected, (list, tuple, set, frozenset)):
            if actual not in expected:
                return False
        elif actual != expected:
            return False
    return True


class PolicyService(PolicyPort):
    def __init__(
        self, events: EventPort | None = None, *, default_effect: str = "allow"
    ) -> None:
        self._rules: dict[str, PolicyRule] = {}
        self._events = events
        self._default_effect = default_effect

    def add_rule(self, rule: PolicyRule) -> None:
        self._rules[rule.id] = rule

    def remove_rule(self, rule_id: str) -> bool:
        return self._rules.pop(rule_id, None) is not None

    def rules(self) -> list[PolicyRule]:
        return list(self._rules.values())

    async def evaluate(
        self,
        *,
        action: str,
        subject: dict[str, Any] | None = None,
        resource: dict[str, Any] | None = None,
        context: dict[str, Any] | None = None,
    ) -> Decision:
        subject = subject or {}
        resource = resource or {}
        context = context or {}
        env = {"subject": subject, "resource": resource, **context}

        matched: list[PolicyRule] = []
        for rule in self._rules.values():
            if not topic_matches(rule.action, action):
                continue
            if not _matches(rule.subject, subject):
                continue
            if not _matches(rule.resource, resource):
                continue
            if rule.condition is not None and not rule.condition(env):
                continue
            matched.append(rule)

        if matched:
            # приоритет по убыванию; deny (0) раньше allow (1) при равенстве
            matched.sort(key=lambda r: (-r.priority, 0 if r.effect == "deny" else 1))
            winner = matched[0]
            decision = Decision(
                allowed=winner.effect == "allow",
                reason=winner.message or f"правило {winner.id}",
                rule_id=winner.id,
                obligations=winner.obligations,
            )
        else:
            decision = Decision(
                allowed=self._default_effect == "allow",
                reason=f"default-{self._default_effect}",
            )

        if self._events:
            await self._events.publish(
                Event(
                    type="policy.decision",
                    payload={
                        "action": action,
                        "allowed": decision.allowed,
                        "rule": decision.rule_id,
                        "subject": {k: str(v) for k, v in subject.items()},
                    },
                )
            )
        return decision


class PolicyRuntime(RuntimeModule):
    def __init__(self, *, default_effect: str = "allow") -> None:
        self._default_effect = default_effect
        self._service: PolicyService | None = None

    def manifest(self) -> ModuleManifest:
        return ModuleManifest(
            id="policy-runtime",
            version="0.2.0",
            provides_ports=("PolicyPort@1",),
            requires_ports=("EventPort@1",),
            provides_events=("policy.decision@1",),
            config_schema={"default_effect": {"type": "string", "enum": ["allow", "deny"]}},
        )

    async def init(self, ctx: ModuleContext) -> None:
        self._service = PolicyService(
            ctx.port("EventPort@1"),
            default_effect=ctx.config.get("default_effect", self._default_effect),
        )
        ctx.register("PolicyPort@1", self._service)

    @property
    def service(self) -> PolicyService:
        assert self._service
        return self._service
