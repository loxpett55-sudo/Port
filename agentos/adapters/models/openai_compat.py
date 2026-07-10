"""Адаптер OpenAI-совместимого HTTP API (chat/completions, embeddings).

Покрывает большинство облачных и локальных серверов (vLLM, llama.cpp
server, Ollama и т.п.). Только стандартная библиотека: urllib в пуле
потоков. Стриминг — SSE.
"""

from __future__ import annotations

import asyncio
import json
import urllib.request
from typing import Any, AsyncIterator, Sequence

from agentos.contracts.errors import TransientError
from agentos.contracts.model import (
    CAP_EMBED,
    CAP_JSON,
    CAP_TEXT,
    CAP_TOOLS,
    GenerateChunk,
    GenerateRequest,
    ModelDescriptor,
    ModelPort,
    ToolCall,
    Usage,
)


class OpenAICompatModel(ModelPort):
    def __init__(
        self,
        *,
        base_url: str,
        model: str,
        api_key: str = "",
        model_id: str = "",
        context_window: int = 128_000,
        cost_in_per_1k: float = 0.0,
        cost_out_per_1k: float = 0.0,
        locality: str = "cloud",
        supports_embed: bool = False,
        timeout: float = 120.0,
    ) -> None:
        self._base = base_url.rstrip("/")
        self._model = model
        self._api_key = api_key
        self._timeout = timeout
        caps = {CAP_TEXT, CAP_TOOLS, CAP_JSON}
        if supports_embed:
            caps.add(CAP_EMBED)
        self._desc = ModelDescriptor(
            id=model_id or f"openai-compat/{model}",
            provider="openai-compat",
            capabilities=frozenset(caps),
            context_window=context_window,
            cost_in_per_1k=cost_in_per_1k,
            cost_out_per_1k=cost_out_per_1k,
            locality=locality,
        )

    def descriptor(self) -> ModelDescriptor:
        return self._desc

    def _http(self, path: str, body: dict[str, Any]) -> Any:
        req = urllib.request.Request(
            f"{self._base}{path}",
            data=json.dumps(body).encode(),
            headers={
                "Content-Type": "application/json",
                **({"Authorization": f"Bearer {self._api_key}"} if self._api_key else {}),
            },
        )
        try:
            with urllib.request.urlopen(req, timeout=self._timeout) as resp:
                return json.loads(resp.read())
        except Exception as e:  # сеть/5xx — временный сбой, ретраит Inference
            raise TransientError(f"openai-compat: {e}") from e

    async def generate(self, request: GenerateRequest) -> AsyncIterator[GenerateChunk]:
        body: dict[str, Any] = {
            "model": self._model,
            "messages": [
                {
                    "role": m.role,
                    "content": m.content,
                    **({"tool_call_id": m.tool_call_id} if m.tool_call_id else {}),
                    **({"name": m.name} if m.name else {}),
                }
                for m in request.messages
            ],
            "max_tokens": request.max_tokens,
            "temperature": request.temperature,
        }
        if request.tools:
            body["tools"] = [
                {
                    "type": "function",
                    "function": {
                        "name": t.name,
                        "description": t.description,
                        "parameters": t.input_schema,
                    },
                }
                for t in request.tools
            ]
        if request.json_mode:
            body["response_format"] = {"type": "json_object"}
        if request.stop:
            body["stop"] = list(request.stop)

        data = await asyncio.to_thread(self._http, "/chat/completions", body)
        choice = data["choices"][0]
        message = choice.get("message", {})
        usage_raw = data.get("usage", {})
        usage = Usage(
            prompt_tokens=usage_raw.get("prompt_tokens", 0),
            completion_tokens=usage_raw.get("completion_tokens", 0),
            cost=(
                usage_raw.get("prompt_tokens", 0) / 1000 * self._desc.cost_in_per_1k
                + usage_raw.get("completion_tokens", 0) / 1000 * self._desc.cost_out_per_1k
            ),
        )
        for tc in message.get("tool_calls") or []:
            yield GenerateChunk(
                tool_call=ToolCall(
                    id=tc.get("id", ""),
                    name=tc["function"]["name"],
                    arguments=json.loads(tc["function"].get("arguments") or "{}"),
                ),
                finish_reason="tool_use",
                usage=usage,
            )
            return
        yield GenerateChunk(
            delta=message.get("content") or "",
            finish_reason=choice.get("finish_reason", "stop"),
            usage=usage,
        )

    async def embed(self, texts: Sequence[str]) -> list[list[float]]:
        data = await asyncio.to_thread(
            self._http, "/embeddings", {"model": self._model, "input": list(texts)}
        )
        return [item["embedding"] for item in data["data"]]
