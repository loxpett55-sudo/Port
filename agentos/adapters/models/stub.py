"""Тестовые адаптеры моделей: детерминированные, без сети и токенов.

StubModel — скриптованные ответы (основа Model Stub из стратегии
тестирования); HashEmbedder — детерминированные эмбеддинги для тестов
векторного поиска.
"""

from __future__ import annotations

import hashlib
import math
import re
from typing import AsyncIterator, Callable, Sequence

from agentos.contracts.model import (
    CAP_EMBED,
    CAP_TEXT,
    CAP_TOOLS,
    ChatMessage,
    GenerateChunk,
    GenerateRequest,
    ModelDescriptor,
    ModelPort,
    ToolCall,
    Usage,
)


def _approx_tokens(text: str) -> int:
    return max(1, len(text) // 4)


class StubModel(ModelPort):
    """Отвечает по сценарию: список ответов (текст или ToolCall) по кругу,
    либо функция от запроса. Стримит по словам."""

    def __init__(
        self,
        replies: Sequence[str | ToolCall] | None = None,
        *,
        script: Callable[[GenerateRequest], str | ToolCall] | None = None,
        model_id: str = "stub-chat",
        cost_in: float = 0.0,
        cost_out: float = 0.0,
        context_window: int = 32768,
        locality: str = "local",
        fail_times: int = 0,
    ) -> None:
        self._replies = list(replies or ["ok"])
        self._script = script
        self._i = 0
        self._desc = ModelDescriptor(
            id=model_id,
            provider="stub",
            capabilities=frozenset({CAP_TEXT, CAP_TOOLS}),
            context_window=context_window,
            cost_in_per_1k=cost_in,
            cost_out_per_1k=cost_out,
            locality=locality,
        )
        self.fail_times = fail_times  # имитация временных сбоев провайдера
        self.calls = 0

    def descriptor(self) -> ModelDescriptor:
        return self._desc

    async def generate(self, request: GenerateRequest) -> AsyncIterator[GenerateChunk]:
        self.calls += 1
        if self.fail_times > 0:
            self.fail_times -= 1
            from agentos.contracts.errors import TransientError

            raise TransientError("stub: имитация сбоя провайдера")

        if self._script:
            reply = self._script(request)
        else:
            reply = self._replies[self._i % len(self._replies)]
            self._i += 1

        prompt_tokens = sum(_approx_tokens(m.content) for m in request.messages)
        if isinstance(reply, ToolCall):
            usage = Usage(prompt_tokens, 10, self._cost(prompt_tokens, 10))
            yield GenerateChunk(tool_call=reply, finish_reason="tool_use", usage=usage)
            return
        words = re.findall(r"\S+\s*", reply) or [""]
        for word in words[:-1]:
            yield GenerateChunk(delta=word)
        completion = _approx_tokens(reply)
        usage = Usage(prompt_tokens, completion, self._cost(prompt_tokens, completion))
        yield GenerateChunk(delta=words[-1], finish_reason="stop", usage=usage)

    def _cost(self, tin: int, tout: int) -> float:
        return tin / 1000 * self._desc.cost_in_per_1k + tout / 1000 * self._desc.cost_out_per_1k


class HashEmbedder(ModelPort):
    """Детерминированный эмбеддер: хэш-проекция по словам. Слова,
    встречающиеся в обоих текстах, дают близкие векторы — достаточно
    для тестов retrieval без настоящей модели."""

    def __init__(self, dim: int = 64, model_id: str = "stub-embed") -> None:
        self._dim = dim
        self._desc = ModelDescriptor(
            id=model_id,
            provider="stub",
            capabilities=frozenset({CAP_EMBED}),
            locality="local",
        )

    def descriptor(self) -> ModelDescriptor:
        return self._desc

    async def generate(self, request: GenerateRequest) -> AsyncIterator[GenerateChunk]:
        raise NotImplementedError("embed-only модель")
        yield  # pragma: no cover

    async def embed(self, texts: Sequence[str]) -> list[list[float]]:
        out = []
        for text in texts:
            vec = [0.0] * self._dim
            for word in re.findall(r"\w+", text.lower()):
                h = int(hashlib.md5(word.encode()).hexdigest(), 16)
                vec[h % self._dim] += 1.0
            norm = math.sqrt(sum(x * x for x in vec)) or 1.0
            out.append([x / norm for x in vec])
        return out


def user(text: str) -> ChatMessage:
    return ChatMessage(role="user", content=text)


def system(text: str) -> ChatMessage:
    return ChatMessage(role="system", content=text)
