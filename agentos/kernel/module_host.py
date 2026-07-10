"""ModuleHost: граф зависимостей и жизненный цикл модулей."""

from __future__ import annotations

import logging
from graphlib import CycleError, TopologicalSorter
from typing import Any

from agentos.contracts.errors import DependencyCycleError, ModuleError
from agentos.contracts.module import (
    HealthStatus,
    ModuleContext,
    ModuleState,
    PortRef,
    RuntimeModule,
)
from agentos.kernel.registry import ServiceRegistry

log = logging.getLogger("agentos.kernel")


class DependencyGraph:
    """Порядок запуска: модуль стартует после всех провайдеров его портов."""

    @staticmethod
    def order(modules: list[RuntimeModule]) -> list[RuntimeModule]:
        by_id = {m.manifest().id: m for m in modules}
        port_owner: dict[str, str] = {}
        for m in modules:
            man = m.manifest()
            for p in man.provides_ports:
                port_owner[PortRef.parse(p).key()] = man.id

        graph: dict[str, set[str]] = {mid: set() for mid in by_id}
        for m in modules:
            man = m.manifest()
            for req in man.requires_ports:
                owner = port_owner.get(PortRef.parse(req).key())
                if owner and owner != man.id:
                    graph[man.id].add(owner)
        try:
            return [by_id[mid] for mid in TopologicalSorter(graph).static_order()]
        except CycleError as e:
            raise DependencyCycleError(f"цикл зависимостей модулей: {e.args[1]}") from e


class ModuleHost:
    def __init__(self, registry: ServiceRegistry, config: dict[str, Any]) -> None:
        self._registry = registry
        self._config = config
        self._modules: list[RuntimeModule] = []
        self._states: dict[str, ModuleState] = {}

    @property
    def modules(self) -> list[RuntimeModule]:
        return list(self._modules)

    def state(self, module_id: str) -> ModuleState:
        return self._states[module_id]

    async def start_all(self, modules: list[RuntimeModule]) -> None:
        ordered = DependencyGraph.order(modules)
        for m in ordered:
            self._states[m.manifest().id] = ModuleState.RESOLVED

        for m in ordered:
            man = m.manifest()
            missing = [
                p for p in man.requires_ports if not self._registry.has(p)
            ]
            if missing:
                self._states[man.id] = ModuleState.FAILED
                raise ModuleError(
                    f"модуль {man.id}: не удовлетворены порты {missing}"
                )
            ctx = ModuleContext(
                module_id=man.id,
                resolve=self._registry.resolve,
                register=lambda spec, provider, owner: self._registry.register(
                    spec, provider, owner=owner
                ),
                config=self._config.get(man.id, {}),
            )
            try:
                await m.init(ctx)
                self._states[man.id] = ModuleState.INITIALIZED
                await m.start()
                self._states[man.id] = ModuleState.RUNNING
                self._modules.append(m)
                log.debug("модуль запущен: %s@%s", man.id, man.version)
            except Exception as e:
                self._states[man.id] = ModuleState.FAILED
                raise ModuleError(f"модуль {man.id}: сбой запуска: {e}") from e

    async def stop_all(self, deadline_seconds: float = 10.0) -> None:
        for m in reversed(self._modules):
            mid = m.manifest().id
            self._states[mid] = ModuleState.STOPPING
            try:
                await m.stop(deadline_seconds)
            except Exception:  # остановка продолжается несмотря на сбои
                log.exception("модуль %s: ошибка при остановке", mid)
            self._states[mid] = ModuleState.STOPPED
        self._modules.clear()

    def health(self) -> dict[str, HealthStatus]:
        return {m.manifest().id: m.health() for m in self._modules}
