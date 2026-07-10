"""Model Runtime — реестр моделей с подбором по требованиям."""

from __future__ import annotations

from typing import Sequence

from agentos.contracts.errors import PortNotFoundError
from agentos.contracts.model import (
    ModelDescriptor,
    ModelPort,
    ModelRegistryPort,
    ModelRequirements,
)
from agentos.contracts.module import ModuleContext, ModuleManifest, RuntimeModule


class ModelRegistry(ModelRegistryPort):
    def __init__(self) -> None:
        self._models: dict[str, ModelPort] = {}
        self._aliases: dict[str, str] = {}

    def register(self, adapter: ModelPort, *, aliases: Sequence[str] = ()) -> None:
        desc = adapter.descriptor()
        self._models[desc.id] = adapter
        for alias in aliases:
            self._aliases[alias] = desc.id

    def get(self, model_id: str) -> ModelPort:
        resolved = self._aliases.get(model_id, model_id)
        try:
            return self._models[resolved]
        except KeyError:
            raise PortNotFoundError(f"модель не зарегистрирована: {model_id}") from None

    def select(self, req: ModelRequirements) -> ModelPort:
        if req.model_id:
            return self.get(req.model_id)
        candidates = [
            m
            for m in self._models.values()
            if req.capabilities <= m.descriptor().capabilities
            and m.descriptor().context_window >= req.min_context
            and (not req.locality or m.descriptor().locality == req.locality)
        ]
        if not candidates:
            raise PortNotFoundError(
                f"нет модели под требования: caps={sorted(req.capabilities)}, "
                f"min_context={req.min_context}, locality={req.locality or 'any'}"
            )
        if req.prefer == "largest-context":
            key = lambda m: -m.descriptor().context_window
        else:  # cheapest
            key = lambda m: (
                m.descriptor().cost_in_per_1k + m.descriptor().cost_out_per_1k
            )
        return sorted(candidates, key=key)[0]

    def list(self) -> list[ModelDescriptor]:
        return [m.descriptor() for m in self._models.values()]


class ModelRuntime(RuntimeModule):
    """Регистрирует ModelRegistryPort; адаптеры моделей добавляются
    плагинами или кодом приложения через реестр."""

    def __init__(self, adapters: Sequence[ModelPort] = ()) -> None:
        self._registry = ModelRegistry()
        self._preload = list(adapters)

    def manifest(self) -> ModuleManifest:
        return ModuleManifest(
            id="model-runtime",
            version="0.2.0",
            provides_ports=("ModelRegistryPort@1",),
        )

    async def init(self, ctx: ModuleContext) -> None:
        for adapter in self._preload:
            self._registry.register(adapter)
        ctx.register("ModelRegistryPort@1", self._registry)

    @property
    def registry(self) -> ModelRegistry:
        return self._registry
