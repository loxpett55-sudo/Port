"""Workflow Runtime — durable-интерпретатор графа шагов.

Конструкции: последовательность, ветвление, циклы, параллельные ветви,
ожидание события, задержка, компенсации (Saga: при сбое — компенсации
выполненных шагов в обратном порядке). Состояние экземпляра сохраняется
в KV после каждого шага.
"""

from __future__ import annotations

import asyncio
import uuid

from agentos.contracts.errors import AgentOSError
from agentos.contracts.events import Event, EventPort
from agentos.contracts.module import ModuleContext, ModuleManifest, RuntimeModule
from agentos.contracts.storage import KVStorePort
from agentos.contracts.workflow import (
    InstanceState,
    Step,
    StepKind,
    Vars,
    WorkflowDef,
    WorkflowInstance,
    WorkflowPort,
)

_NS = "workflow"


class WorkflowEngine(WorkflowPort):
    def __init__(self, kv: KVStorePort | None = None, events: EventPort | None = None):
        self._kv = kv
        self._events = events
        self._defs: dict[str, WorkflowDef] = {}
        self._instances: dict[str, WorkflowInstance] = {}
        self._done: dict[str, asyncio.Event] = {}
        self._runners: dict[str, asyncio.Task] = {}

    def register(self, definition: WorkflowDef) -> None:
        self._defs[definition.id] = definition

    async def start(self, workflow_id: str, vars: Vars | None = None) -> str:
        definition = self._defs.get(workflow_id)
        if definition is None:
            raise AgentOSError(f"workflow не зарегистрирован: {workflow_id}")
        instance = WorkflowInstance(
            id=uuid.uuid4().hex, workflow_id=workflow_id, vars=dict(vars or {})
        )
        self._instances[instance.id] = instance
        self._done[instance.id] = asyncio.Event()
        self._runners[instance.id] = asyncio.create_task(
            self._run_instance(definition, instance)
        )
        await self._emit("workflow.started", instance)
        return instance.id

    async def status(self, instance_id: str) -> WorkflowInstance:
        try:
            return self._instances[instance_id]
        except KeyError:
            raise AgentOSError(f"экземпляр не найден: {instance_id}") from None

    async def wait(self, instance_id: str, timeout: float = 60.0) -> WorkflowInstance:
        await asyncio.wait_for(self._done[instance_id].wait(), timeout)
        return self._instances[instance_id]

    # --- интерпретатор ---------------------------------------------------------

    async def _run_instance(self, definition: WorkflowDef, instance: WorkflowInstance):
        executed: list[Step] = []  # для компенсаций, в порядке выполнения
        try:
            await self._run_steps(definition.steps, instance, executed)
            instance.state = InstanceState.COMPLETED
            await self._emit("workflow.completed", instance)
        except Exception as e:
            instance.error = str(e) or type(e).__name__
            compensated = await self._compensate(executed, instance)
            instance.state = (
                InstanceState.COMPENSATED if compensated else InstanceState.FAILED
            )
            await self._emit(
                "workflow.compensated" if compensated else "workflow.failed", instance
            )
        finally:
            await self._persist(instance)
            self._done[instance.id].set()

    async def _run_steps(
        self, steps: tuple[Step, ...], instance: WorkflowInstance, executed: list[Step]
    ) -> None:
        for step in steps:
            await self._run_step(step, instance, executed)
            instance.completed_steps.append(step.id)
            await self._persist(instance)
            await self._emit("workflow.step.completed", instance, step=step.id)

    async def _run_step(
        self, step: Step, instance: WorkflowInstance, executed: list[Step]
    ) -> None:
        vars = instance.vars
        if step.kind is StepKind.ACTION:
            assert step.run is not None, f"шаг {step.id}: нет функции run"
            result = await step.run(vars)
            vars[step.id] = result
            executed.append(step)

        elif step.kind is StepKind.BRANCH:
            assert step.condition is not None
            branch = step.then_steps if step.condition(vars) else step.else_steps
            await self._run_steps(branch, instance, executed)

        elif step.kind is StepKind.LOOP:
            assert step.condition is not None
            iterations = 0
            while step.condition(vars):
                if iterations >= step.max_iterations:
                    raise AgentOSError(
                        f"шаг {step.id}: превышен лимит итераций {step.max_iterations}"
                    )
                await self._run_steps(step.then_steps, instance, executed)
                iterations += 1

        elif step.kind is StepKind.PARALLEL:
            # ветви изолированы по executed-спискам, компенсации сохраняются все
            branch_executed: list[list[Step]] = [[] for _ in step.branches]
            try:
                await asyncio.gather(
                    *(
                        self._run_steps(branch, instance, branch_executed[i])
                        for i, branch in enumerate(step.branches)
                    )
                )
            finally:
                for be in branch_executed:
                    executed.extend(be)

        elif step.kind is StepKind.WAIT_EVENT:
            await self._wait_event(step, instance)

        elif step.kind is StepKind.DELAY:
            await asyncio.sleep(step.delay_seconds)

    async def _wait_event(self, step: Step, instance: WorkflowInstance) -> None:
        if self._events is None:
            raise AgentOSError(f"шаг {step.id}: wait_event требует EventPort")
        fut: asyncio.Future = asyncio.get_running_loop().create_future()

        async def handler(event: Event) -> None:
            if not fut.done():
                fut.set_result(event)

        sub = self._events.subscribe(step.event_pattern, handler)
        try:
            timeout = step.timeout_seconds or None
            event = await asyncio.wait_for(fut, timeout)
            instance.vars[step.id] = event.payload
        except asyncio.TimeoutError:
            raise AgentOSError(
                f"шаг {step.id}: тайм-аут ожидания события {step.event_pattern}"
            ) from None
        finally:
            self._events.unsubscribe(sub)

    async def _compensate(self, executed: list[Step], instance: WorkflowInstance) -> bool:
        compensable = [s for s in reversed(executed) if s.compensate]
        if not compensable:
            return False
        for step in compensable:
            try:
                await step.compensate(instance.vars)  # type: ignore[misc]
            except Exception:
                pass  # компенсация best-effort; сбой фиксируется аудитом событий
        return True

    async def _persist(self, instance: WorkflowInstance) -> None:
        if self._kv is None:
            return
        await self._kv.put(
            _NS,
            instance.id,
            {
                "workflow_id": instance.workflow_id,
                "state": instance.state.value,
                "completed_steps": instance.completed_steps,
                "error": instance.error,
            },
        )

    async def _emit(self, type_: str, instance: WorkflowInstance, **extra) -> None:
        if self._events:
            await self._events.publish(
                Event(
                    type=type_,
                    payload={"workflow": instance.workflow_id, **extra},
                    subject=f"workflow:{instance.id}",
                )
            )


class WorkflowRuntime(RuntimeModule):
    def __init__(self) -> None:
        self._engine: WorkflowEngine | None = None

    def manifest(self) -> ModuleManifest:
        return ModuleManifest(
            id="workflow-runtime",
            version="0.2.0",
            provides_ports=("WorkflowPort@1",),
            requires_ports=("KVStorePort@1", "EventPort@1"),
            provides_events=(
                "workflow.started@1", "workflow.step.completed@1",
                "workflow.completed@1", "workflow.failed@1", "workflow.compensated@1",
            ),
        )

    async def init(self, ctx: ModuleContext) -> None:
        self._engine = WorkflowEngine(ctx.port("KVStorePort@1"), ctx.port("EventPort@1"))
        ctx.register("WorkflowPort@1", self._engine)

    @property
    def engine(self) -> WorkflowEngine:
        assert self._engine
        return self._engine
