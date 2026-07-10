"""API Runtime — внешняя поверхность платформы.

Тонкий слой: транслирует HTTP/JSON в порты и события, бизнес-логики не
содержит. Эталонный сервер — HTTP/1.1 на asyncio (без зависимостей);
gRPC/WebSocket-транспорты подключаются адаптерами. Единая модель ошибок:
{error, retryable, trace_id}.
"""

from __future__ import annotations

import asyncio
import json
import re
import uuid
from pathlib import Path
from typing import Any, Awaitable, Callable

from agentos.contracts.agent import AgentDefinition
from agentos.contracts.errors import AgentOSError, PermissionDeniedError, PortNotFoundError
from agentos.contracts.knowledge import Document, TrustLevel
from agentos.contracts.module import ModuleContext, ModuleManifest, RuntimeModule

Handler = Callable[[dict[str, str], dict[str, Any]], Awaitable[tuple[int, Any]]]

_DASHBOARD = Path(__file__).with_name("dashboard.html")


class HttpApiServer:
    """Минимальный HTTP/1.1 сервер с маршрутизацией `METHOD /path/{param}`."""

    def __init__(self, platform: Any) -> None:
        self.platform = platform
        self._routes: list[tuple[str, re.Pattern, Handler]] = []
        self._server: asyncio.Server | None = None
        self.port: int = 0
        self._register_routes()

    # --- маршруты ---------------------------------------------------------

    def route(self, method: str, pattern: str, handler: Handler) -> None:
        regex = re.compile(
            "^" + re.sub(r"\{(\w+)\}", r"(?P<\1>[^/]+)", pattern) + "$"
        )
        self._routes.append((method, regex, handler))

    def _register_routes(self) -> None:
        p = self.platform
        self.route("GET", "/v1/health", self._health)
        self.route("GET", "/v1/models", self._models)
        self.route("GET", "/v1/tools", self._tools)
        self.route("GET", "/v1/agents", self._agents)
        self.route("POST", "/v1/agents", self._register_agent)
        self.route("POST", "/v1/agents/{agent_id}/messages", self._message)
        self.route("POST", "/v1/sessions", self._open_session)
        self.route("GET", "/v1/sessions/{session_id}/turns", self._turns)
        self.route("GET", "/v1/events", self._events)
        self.route("GET", "/v1/metrics", self._metrics)
        self.route("POST", "/v1/knowledge/{kb}/documents", self._add_document)
        self.route("GET", "/v1/knowledge/search", self._search_knowledge)

    # --- обработчики ---------------------------------------------------------

    async def _health(self, params, body):
        return 200, {"status": "ok", "modules": self.platform.health()}

    async def _models(self, params, body):
        return 200, {
            "models": [
                {
                    "id": d.id,
                    "provider": d.provider,
                    "capabilities": sorted(d.capabilities),
                    "context_window": d.context_window,
                    "locality": d.locality,
                }
                for d in self.platform.models.list()
            ]
        }

    async def _tools(self, params, body):
        return 200, {
            "tools": [
                {"id": m.id, "version": m.version, "description": m.description}
                for m in self.platform.tools.list()
            ]
        }

    async def _agents(self, params, body):
        return 200, {
            "agents": [
                {
                    "id": d.id,
                    "personality": d.personality,
                    "goals": list(d.goals),
                    "tools": list(d.tools),
                    "reasoner": d.reasoner,
                }
                for d in self.platform.agents.definitions()
            ]
        }

    async def _register_agent(self, params, body):
        definition = AgentDefinition(
            id=body["id"],
            personality=body.get("personality", "Полезный ассистент."),
            goals=tuple(body.get("goals", ())),
            tools=tuple(body.get("tools", ())),
            max_iterations=int(body.get("max_iterations", 8)),
        )
        self.platform.agents.register(definition)
        return 201, {"id": definition.id}

    async def _message(self, params, body):
        reply = await self.platform.agents.send(
            params["agent_id"],
            body["content"],
            session_id=body.get("session_id", ""),
        )
        return 200, {
            "text": reply.text,
            "iterations": reply.iterations,
            "tool_calls": reply.tool_calls,
        }

    async def _open_session(self, params, body):
        session = await self.platform.sessions.open(
            body.get("principal", "anonymous"), body.get("metadata")
        )
        return 201, {"id": session.id, "principal": session.principal}

    async def _turns(self, params, body):
        turns = await self.platform.sessions.turns(params["session_id"])
        return 200, {
            "turns": [
                {"role": t.role, "content": t.content, "agent_id": t.agent_id}
                for t in turns
            ]
        }

    async def _events(self, params, body):
        history = await self.platform.events.history(
            params.get("pattern", "**"), limit=int(params.get("limit", 100))
        )
        return 200, {
            "events": [
                {"type": e.type, "time": e.time, "subject": e.subject,
                 "payload": e.payload}
                for e in history
            ]
        }

    async def _metrics(self, params, body):
        return 200, self.platform.metrics.snapshot()

    async def _add_document(self, params, body):
        chunks = await self.platform.knowledge.add_document(
            params["kb"],
            Document(
                text=body["text"],
                source=body.get("source", "api"),
                title=body.get("title", ""),
                trust=TrustLevel(body.get("trust", "internal")),
            ),
        )
        return 201, {"kb": params["kb"], "chunks": chunks}

    async def _search_knowledge(self, params, body):
        hits = await self.platform.knowledge.retrieve(
            params.get("q", ""), k=int(params.get("k", 8))
        )
        return 200, {
            "hits": [
                {"text": h.text, "score": round(h.score, 4), "source": h.source,
                 "trust": h.trust.value}
                for h in hits
            ]
        }

    # --- HTTP-сервер ------------------------------------------------------------

    async def start(self, host: str = "127.0.0.1", port: int = 0) -> None:
        self._server = await asyncio.start_server(self._handle, host, port)
        self.port = self._server.sockets[0].getsockname()[1]

    async def stop(self) -> None:
        if self._server:
            self._server.close()
            await self._server.wait_closed()

    async def _handle(self, reader: asyncio.StreamReader, writer: asyncio.StreamWriter):
        try:
            request_line = (await reader.readline()).decode()
            if not request_line.strip():
                return
            method, target, _ = request_line.split(" ", 2)
            headers: dict[str, str] = {}
            while True:
                line = (await reader.readline()).decode()
                if line in ("\r\n", "\n", ""):
                    break
                key, _, value = line.partition(":")
                headers[key.strip().lower()] = value.strip()
            body_raw = b""
            if length := int(headers.get("content-length", 0)):
                body_raw = await reader.readexactly(length)

            status, payload, content_type = await self._dispatch(method, target, body_raw)
            data = (
                payload.encode()
                if isinstance(payload, str)
                else json.dumps(payload, ensure_ascii=False).encode()
            )
            writer.write(
                f"HTTP/1.1 {status} {'OK' if status < 400 else 'Error'}\r\n"
                f"Content-Type: {content_type}\r\n"
                f"Content-Length: {len(data)}\r\n"
                "Connection: close\r\n\r\n".encode() + data
            )
            await writer.drain()
        except Exception:
            pass
        finally:
            writer.close()

    async def _dispatch(self, method: str, target: str, body_raw: bytes):
        from urllib.parse import parse_qsl, unquote

        path, _, query = target.partition("?")
        path = unquote(path)
        params = dict(parse_qsl(query))
        if method == "GET" and path == "/":
            return 200, _DASHBOARD.read_text(), "text/html; charset=utf-8"

        body: dict[str, Any] = {}
        if body_raw:
            try:
                body = json.loads(body_raw)
            except json.JSONDecodeError:
                return 400, {"error": "невалидный JSON"}, "application/json"

        for route_method, regex, handler in self._routes:
            if route_method != method:
                continue
            match = regex.match(path)
            if match is None:
                continue
            try:
                status, payload = await handler({**params, **match.groupdict()}, body)
                return status, payload, "application/json"
            except PermissionDeniedError as e:
                return 403, _err(e), "application/json"
            except (PortNotFoundError, KeyError) as e:
                return 404, _err(e), "application/json"
            except AgentOSError as e:
                return 500, _err(e), "application/json"
        return 404, {"error": f"нет маршрута: {method} {path}"}, "application/json"


def _err(e: Exception) -> dict[str, Any]:
    return {
        "error": str(e),
        "retryable": getattr(e, "retryable", False),
        "trace_id": uuid.uuid4().hex,
    }


class ApiRuntime(RuntimeModule):
    """Модуль-обёртка: поднимает HTTP-сервер поверх портов платформы."""

    def __init__(self, platform: Any, *, host: str = "127.0.0.1", port: int = 8080):
        self._platform = platform
        self._host = host
        self._port = port
        self.server: HttpApiServer | None = None

    def manifest(self) -> ModuleManifest:
        return ModuleManifest(
            id="api-runtime",
            version="0.2.0",
            requires_ports=("EventPort@1",),
            optional_ports=("AgentPort@1", "SessionPort@1", "KnowledgePort@1"),
            config_schema={"host": {"type": "string"}, "port": {"type": "integer"}},
        )

    async def init(self, ctx: ModuleContext) -> None:
        self._host = ctx.config.get("host", self._host)
        self._port = int(ctx.config.get("port", self._port))
        self.server = HttpApiServer(self._platform)

    async def start(self) -> None:
        assert self.server
        await self.server.start(self._host, self._port)

    async def stop(self, deadline_seconds: float = 10.0) -> None:
        if self.server:
            await self.server.stop()
