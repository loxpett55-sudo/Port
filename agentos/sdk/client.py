"""HTTP-клиент к платформе (Python SDK, remote-режим).

Синхронный, на stdlib; асинхронная обёртка — через asyncio.to_thread.
"""

from __future__ import annotations

import json
import urllib.request
from typing import Any

from agentos.contracts.errors import AgentOSError


class AgentOSClient:
    def __init__(self, base_url: str, *, timeout: float = 60.0) -> None:
        self._base = base_url.rstrip("/")
        self._timeout = timeout

    # --- низкий уровень -----------------------------------------------------

    def request(self, method: str, path: str, body: dict[str, Any] | None = None) -> Any:
        data = json.dumps(body).encode() if body is not None else None
        req = urllib.request.Request(
            f"{self._base}{path}",
            data=data,
            method=method,
            headers={"Content-Type": "application/json"},
        )
        try:
            with urllib.request.urlopen(req, timeout=self._timeout) as resp:
                return json.loads(resp.read())
        except urllib.error.HTTPError as e:
            try:
                payload = json.loads(e.read())
            except Exception:
                payload = {"error": str(e)}
            raise AgentOSError(
                payload.get("error", str(e)),
                retryable=payload.get("retryable", False),
            ) from None

    # --- высокоуровневые операции ------------------------------------------------

    def health(self) -> dict[str, Any]:
        return self.request("GET", "/v1/health")

    def models(self) -> list[dict[str, Any]]:
        return self.request("GET", "/v1/models")["models"]

    def agents(self) -> list[dict[str, Any]]:
        return self.request("GET", "/v1/agents")["agents"]

    def register_agent(self, id: str, **kwargs: Any) -> str:
        return self.request("POST", "/v1/agents", {"id": id, **kwargs})["id"]

    def open_session(self, principal: str = "sdk-user") -> str:
        return self.request("POST", "/v1/sessions", {"principal": principal})["id"]

    def send(self, agent_id: str, content: str, *, session_id: str = "") -> dict[str, Any]:
        return self.request(
            "POST",
            f"/v1/agents/{agent_id}/messages",
            {"content": content, "session_id": session_id},
        )

    def add_document(self, kb: str, text: str, *, source: str = "sdk", trust: str = "internal"):
        return self.request(
            "POST", f"/v1/knowledge/{kb}/documents",
            {"text": text, "source": source, "trust": trust},
        )

    def search(self, query: str, k: int = 8) -> list[dict[str, Any]]:
        from urllib.parse import quote

        return self.request("GET", f"/v1/knowledge/search?q={quote(query)}&k={k}")["hits"]

    def metrics(self) -> dict[str, Any]:
        return self.request("GET", "/v1/metrics")

    def events(self, pattern: str = "**", limit: int = 100) -> list[dict[str, Any]]:
        from urllib.parse import quote

        return self.request("GET", f"/v1/events?pattern={quote(pattern)}&limit={limit}")[
            "events"
        ]
