"""Security Runtime: секреты (короткоживущие lease), append-only аудит с
хэш-сцеплением, детектор угроз по потоку событий.

Шифрование секретов at-rest — эталонный потоковый шифр на HMAC-SHA256
(keystream) с проверкой целостности; продакшн-шифрование (AES-GCM, KMS)
подключается адаптером CipherPort. Sandbox-уровни process/container —
адаптеры; enforcement тайм-аутов выполняет Tool Runtime.
"""

from __future__ import annotations

import base64
import hashlib
import hmac
import json
import secrets as pysecrets
import time
from collections import deque
from typing import Any

from agentos.contracts.errors import AgentOSError, PermissionDeniedError
from agentos.contracts.events import Event, EventPort
from agentos.contracts.module import ModuleContext, ModuleManifest, RuntimeModule
from agentos.contracts.security import AuditPort, AuditRecord, SecretLease, SecretsPort
from agentos.contracts.storage import KVStorePort

_NS = "secrets"


class _ReferenceCipher:
    """Эталонный шифр: HMAC-SHA256 keystream + тег целостности.
    Заменяется продакшн-адаптером (AES-GCM/KMS) через CipherPort."""

    def __init__(self, master_key: bytes) -> None:
        self._key = master_key

    def encrypt(self, plaintext: bytes) -> str:
        nonce = pysecrets.token_bytes(16)
        stream = self._keystream(nonce, len(plaintext))
        ct = bytes(a ^ b for a, b in zip(plaintext, stream))
        tag = hmac.new(self._key, nonce + ct, hashlib.sha256).digest()
        return base64.b64encode(nonce + ct + tag).decode()

    def decrypt(self, blob: str) -> bytes:
        raw = base64.b64decode(blob)
        nonce, ct, tag = raw[:16], raw[16:-32], raw[-32:]
        expected = hmac.new(self._key, nonce + ct, hashlib.sha256).digest()
        if not hmac.compare_digest(tag, expected):
            raise AgentOSError("секрет повреждён: нарушена целостность")
        stream = self._keystream(nonce, len(ct))
        return bytes(a ^ b for a, b in zip(ct, stream))

    def _keystream(self, nonce: bytes, length: int) -> bytes:
        out = b""
        counter = 0
        while len(out) < length:
            out += hmac.new(
                self._key, nonce + counter.to_bytes(8, "big"), hashlib.sha256
            ).digest()
            counter += 1
        return out[:length]


class SecretsVault(SecretsPort):
    """Секреты: шифрование at-rest, выдача короткоживущими lease,
    каждая выдача — в аудит."""

    def __init__(
        self, kv: KVStorePort, audit: "AuditLog | None" = None, master_key: bytes | None = None
    ) -> None:
        self._kv = kv
        self._audit = audit
        self._cipher = _ReferenceCipher(master_key or pysecrets.token_bytes(32))

    async def put(self, name: str, value: str) -> None:
        await self._kv.put(_NS, name, self._cipher.encrypt(value.encode()))
        if self._audit:
            await self._audit.record("system", "secret.put", {"name": name})

    async def lease(self, name: str, *, ttl: float = 60.0, actor: str = "") -> SecretLease:
        blob = await self._kv.get(_NS, name)
        if blob is None:
            raise AgentOSError(f"секрет не найден: {name}")
        value = self._cipher.decrypt(blob).decode()
        if self._audit:
            # значение секрета в аудит не попадает — только факт выдачи
            await self._audit.record(actor or "unknown", "secret.lease", {"name": name, "ttl": ttl})
        return SecretLease(name=name, value=value, expires=time.time() + ttl)

    async def delete(self, name: str) -> bool:
        deleted = await self._kv.delete(_NS, name)
        if deleted and self._audit:
            await self._audit.record("system", "secret.delete", {"name": name})
        return deleted


class AuditLog(AuditPort):
    """Append-only журнал: каждая запись сцеплена хэшем с предыдущей —
    подмена или удаление записи ломает цепочку."""

    def __init__(self) -> None:
        self._records: list[AuditRecord] = []

    async def record(self, actor: str, action: str, detail: dict[str, Any]) -> AuditRecord:
        prev_hash = self._records[-1].hash if self._records else "genesis"
        index = len(self._records)
        ts = time.time()
        digest = hashlib.sha256(
            json.dumps(
                [index, ts, actor, action, detail, prev_hash],
                sort_keys=True,
                ensure_ascii=False,
            ).encode()
        ).hexdigest()
        rec = AuditRecord(
            index=index, time=ts, actor=actor, action=action,
            detail=detail, prev_hash=prev_hash, hash=digest,
        )
        self._records.append(rec)
        return rec

    async def read(self, *, since_index: int = 0, limit: int = 1000) -> list[AuditRecord]:
        return [r for r in self._records if r.index >= since_index][:limit]

    async def verify(self) -> bool:
        prev = "genesis"
        for rec in self._records:
            expected = hashlib.sha256(
                json.dumps(
                    [rec.index, rec.time, rec.actor, rec.action, rec.detail, prev],
                    sort_keys=True,
                    ensure_ascii=False,
                ).encode()
            ).hexdigest()
            if rec.prev_hash != prev or rec.hash != expected:
                return False
            prev = rec.hash
        return True


class ThreatDetector:
    """Поведенческий детектор: аномальная частота отказов/сбоев по
    subject → событие security.threat.detected. Новые детекторы —
    плагины-подписчики шины."""

    def __init__(
        self,
        events: EventPort,
        *,
        window_seconds: float = 10.0,
        deny_threshold: int = 5,
    ) -> None:
        self._events = events
        self._window = window_seconds
        self._threshold = deny_threshold
        self._denials: dict[str, deque[float]] = {}
        self._alerted: set[str] = set()

    def start(self) -> None:
        self._events.subscribe("tool.denied", self._on_denied)
        self._events.subscribe("policy.decision", self._on_decision)

    async def _on_decision(self, event: Event) -> None:
        if not event.payload.get("allowed", True):
            actor = str(event.payload.get("subject", {}).get("agent_id", "unknown"))
            await self._track(actor)

    async def _on_denied(self, event: Event) -> None:
        await self._track(str(event.payload.get("agent_id", "unknown")))

    async def _track(self, actor: str) -> None:
        now = time.time()
        window = self._denials.setdefault(actor, deque())
        window.append(now)
        while window and window[0] < now - self._window:
            window.popleft()
        if len(window) >= self._threshold and actor not in self._alerted:
            self._alerted.add(actor)
            await self._events.publish(
                Event(
                    type="security.threat.detected",
                    payload={
                        "actor": actor,
                        "kind": "excessive-denials",
                        "count": len(window),
                        "window_seconds": self._window,
                    },
                )
            )


class SecurityRuntime(RuntimeModule):
    def __init__(self, master_key: bytes | None = None) -> None:
        self._master_key = master_key
        self.audit: AuditLog | None = None
        self.secrets: SecretsVault | None = None
        self.threats: ThreatDetector | None = None

    def manifest(self) -> ModuleManifest:
        return ModuleManifest(
            id="security-runtime",
            version="0.2.0",
            provides_ports=("SecretsPort@1", "AuditPort@1"),
            requires_ports=("KVStorePort@1", "EventPort@1"),
            provides_events=("security.threat.detected@1",),
        )

    async def init(self, ctx: ModuleContext) -> None:
        events = ctx.port("EventPort@1")
        self.audit = AuditLog()
        self.secrets = SecretsVault(ctx.port("KVStorePort@1"), self.audit, self._master_key)
        self.threats = ThreatDetector(events)
        ctx.register("AuditPort@1", self.audit)
        ctx.register("SecretsPort@1", self.secrets)

    async def start(self) -> None:
        assert self.threats
        self.threats.start()
