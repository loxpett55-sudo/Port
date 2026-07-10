"""Composition root: сборка полной платформы из Runtime-модулей.

`AgentOSPlatform` — удобная «дистрибуция» modular monolith (профиль T1/T2):
поднимает ядро со всеми модулями и даёт типизированный доступ к портам.
Любой модуль можно исключить или заменить — контракты не меняются.
"""

from __future__ import annotations

from typing import Any, Sequence

from agentos.contracts.model import ModelPort
from agentos.kernel import Kernel
from agentos.runtimes.agent import AgentRuntime
from agentos.runtimes.context import ContextRuntime
from agentos.runtimes.event import EventRuntime
from agentos.runtimes.inference import InferenceRuntime
from agentos.runtimes.knowledge import KnowledgeRuntime
from agentos.runtimes.memory import MemoryRuntime
from agentos.runtimes.model import ModelRuntime
from agentos.runtimes.observability import ObservabilityRuntime
from agentos.runtimes.policy import PolicyRuntime
from agentos.runtimes.resource import ResourceRuntime
from agentos.runtimes.scheduler import SchedulerRuntime
from agentos.runtimes.security import SecurityRuntime
from agentos.runtimes.session import SessionRuntime
from agentos.runtimes.storage import StorageRuntime
from agentos.runtimes.task import TaskRuntime
from agentos.runtimes.tool import ToolRuntime
from agentos.runtimes.workflow import WorkflowRuntime


class AgentOSPlatform:
    def __init__(
        self,
        config: dict[str, Any] | None = None,
        *,
        models: Sequence[ModelPort] = (),
    ) -> None:
        self.kernel = Kernel(config or {})
        self._model_runtime = ModelRuntime(models)
        self._modules = [
            StorageRuntime(),
            EventRuntime(),
            ObservabilityRuntime(),
            PolicyRuntime(),
            SecurityRuntime(),
            ResourceRuntime(),
            self._model_runtime,
            InferenceRuntime(),
            ToolRuntime(),
            SessionRuntime(),
            MemoryRuntime(),
            KnowledgeRuntime(),
            ContextRuntime(),
            TaskRuntime(),
            SchedulerRuntime(),
            WorkflowRuntime(),
            AgentRuntime(),
        ]

    async def start(self) -> None:
        await self.kernel.boot(self._modules)

    async def stop(self) -> None:
        await self.kernel.shutdown()

    # --- типизированный доступ к портам -----------------------------------

    def port(self, spec: str) -> Any:
        return self.kernel.registry.resolve(spec)

    @property
    def events(self):
        return self.port("EventPort@1")

    @property
    def models(self):
        return self.port("ModelRegistryPort@1")

    @property
    def inference(self):
        return self.port("InferencePort@1")

    @property
    def tools(self):
        return self.port("ToolPort@1")

    @property
    def sessions(self):
        return self.port("SessionPort@1")

    @property
    def memory(self):
        return self.port("MemoryPort@1")

    @property
    def knowledge(self):
        return self.port("KnowledgePort@1")

    @property
    def context(self):
        return self.port("ContextPort@1")

    @property
    def tasks(self):
        return self.port("TaskPort@1")

    @property
    def scheduler(self):
        return self.port("SchedulerPort@1")

    @property
    def workflows(self):
        return self.port("WorkflowPort@1")

    @property
    def agents(self):
        return self.port("AgentPort@1")

    @property
    def policy(self):
        return self.port("PolicyPort@1")

    @property
    def secrets(self):
        return self.port("SecretsPort@1")

    @property
    def audit(self):
        return self.port("AuditPort@1")

    @property
    def resources(self):
        return self.port("ResourcePort@1")

    @property
    def metrics(self):
        return self.port("MetricsPort@1")

    def health(self) -> dict[str, Any]:
        return {
            module_id: {"state": status.state.value, "detail": status.detail}
            for module_id, status in self.kernel.health().items()
        }
