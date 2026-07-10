"""Kernel — точка входа платформы.

Отвечает только за: bootstrap-конфигурацию, DI/реестр сервисов,
жизненный цикл модулей. Всё остальное — модули.
"""

from __future__ import annotations

import enum
import json
import logging
import os
from pathlib import Path
from typing import Any

from agentos.contracts.module import HealthStatus, RuntimeModule
from agentos.kernel.module_host import ModuleHost
from agentos.kernel.registry import ServiceRegistry

log = logging.getLogger("agentos.kernel")


class KernelState(enum.Enum):
    CREATED = "created"
    RUNNING = "running"
    STOPPED = "stopped"


class BootstrapConfig:
    """Минимальный загрузчик: JSON-файл + переменные окружения AGENTOS_*.

    Полноценная конфигурация с hot-reload — задача Configuration Runtime;
    ядру нужно лишь «что грузить и как соединять».
    """

    @staticmethod
    def load(path: str | Path | None = None) -> dict[str, Any]:
        config: dict[str, Any] = {}
        if path and Path(path).exists():
            config = json.loads(Path(path).read_text())
        for key, value in os.environ.items():
            if key.startswith("AGENTOS_"):
                # AGENTOS_EVENT_RUNTIME__HISTORY_LIMIT=500 →
                # config["event-runtime"]["history_limit"] = "500"
                raw = key[len("AGENTOS_") :].lower()
                module, _, param = raw.partition("__")
                if param:
                    config.setdefault(module.replace("_", "-"), {})[param] = value
        return config


class Kernel:
    def __init__(self, config: dict[str, Any] | None = None) -> None:
        self.config = config or {}
        self.registry = ServiceRegistry()
        self.host = ModuleHost(self.registry, self.config)
        self.state = KernelState.CREATED

    async def boot(self, modules: list[RuntimeModule]) -> None:
        if self.state is KernelState.RUNNING:
            return
        log.info("AgentOS: запуск, модулей: %d", len(modules))
        await self.host.start_all(modules)
        self.state = KernelState.RUNNING

    async def shutdown(self, deadline_seconds: float = 10.0) -> None:
        if self.state is not KernelState.RUNNING:
            return
        await self.host.stop_all(deadline_seconds)
        self.state = KernelState.STOPPED
        log.info("AgentOS: остановлен")

    def health(self) -> dict[str, HealthStatus]:
        return self.host.health()
