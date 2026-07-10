"""Scheduler Runtime — таймеры, периодические задачи и DAG зависимостей.

Планировщик решает «когда», Task Runtime — «как исполнить». Узел DAG
стартует после успеха всех зависимостей; сбой узла проваливает
зависимые узлы, независимые ветви продолжаются.
"""

from __future__ import annotations

import asyncio
import dataclasses
import heapq
import itertools
import time
import uuid

from agentos.contracts.errors import AgentOSError
from agentos.contracts.events import Event, EventPort
from agentos.contracts.module import ModuleContext, ModuleManifest, RuntimeModule
from agentos.contracts.scheduler import DagSpec, DagStatus, SchedulerPort
from agentos.contracts.task import TaskPort, TaskSpec


class SchedulerService(SchedulerPort):
    def __init__(self, tasks: TaskPort, events: EventPort | None = None) -> None:
        self._tasks = tasks
        self._events = events
        # (срок, seq, timer_id, spec, interval|None)
        self._timers: list[tuple[float, int, str, TaskSpec, float | None]] = []
        self._cancelled: set[str] = set()
        self._seq = itertools.count()
        self._dags: dict[str, DagStatus] = {}
        self._dag_tasks: dict[str, asyncio.Task] = {}
        self._loop_task: asyncio.Task | None = None
        self._running = False

    # --- жизненный цикл ------------------------------------------------------

    async def start(self) -> None:
        self._running = True
        self._loop_task = asyncio.create_task(self._timer_loop())

    async def stop(self) -> None:
        self._running = False
        for t in [self._loop_task, *self._dag_tasks.values()]:
            if t:
                t.cancel()
                try:
                    await t
                except asyncio.CancelledError:
                    pass

    # --- таймеры ----------------------------------------------------------------

    async def at(self, when: float, spec: TaskSpec) -> str:
        timer_id = uuid.uuid4().hex
        heapq.heappush(self._timers, (when, next(self._seq), timer_id, spec, None))
        return timer_id

    async def every(self, interval_seconds: float, spec: TaskSpec) -> str:
        timer_id = uuid.uuid4().hex
        heapq.heappush(
            self._timers,
            (time.time() + interval_seconds, next(self._seq), timer_id, spec, interval_seconds),
        )
        return timer_id

    async def cancel_timer(self, timer_id: str) -> bool:
        self._cancelled.add(timer_id)
        return True

    async def _timer_loop(self) -> None:
        while self._running:
            now = time.time()
            while self._timers and self._timers[0][0] <= now:
                when, _, timer_id, spec, interval = heapq.heappop(self._timers)
                if timer_id in self._cancelled:
                    continue
                # каждое срабатывание — новая задача (новый id)
                fired = dataclasses.replace(spec, id=uuid.uuid4().hex)
                await self._tasks.submit(fired)
                if self._events:
                    await self._events.publish(
                        Event(type="scheduler.timer.fired",
                              payload={"timer_id": timer_id, "task_type": spec.type})
                    )
                if interval is not None:
                    heapq.heappush(
                        self._timers,
                        (now + interval, next(self._seq), timer_id, spec, interval),
                    )
            delay = (self._timers[0][0] - now) if self._timers else 0.5
            await asyncio.sleep(min(max(delay, 0.005), 0.5))

    # --- DAG ------------------------------------------------------------------

    async def submit_dag(self, dag: DagSpec) -> str:
        for node, deps in dag.dependencies.items():
            unknown = [d for d in (*deps, node) if d not in dag.tasks]
            if unknown:
                raise AgentOSError(f"DAG: неизвестные узлы {unknown}")
        dag_id = uuid.uuid4().hex
        status = DagStatus(
            dag_id=dag_id,
            done=False,
            node_states={n: "pending" for n in dag.tasks},
        )
        self._dags[dag_id] = status
        self._dag_tasks[dag_id] = asyncio.create_task(self._run_dag(dag, status))
        return dag_id

    async def dag_status(self, dag_id: str) -> DagStatus:
        try:
            return self._dags[dag_id]
        except KeyError:
            raise AgentOSError(f"DAG не найден: {dag_id}") from None

    async def _run_dag(self, dag: DagSpec, status: DagStatus) -> None:
        remaining = dict(dag.tasks)
        failed_nodes: set[str] = set()

        async def run_node(name: str, spec: TaskSpec) -> None:
            status.node_states[name] = "running"
            try:
                task_id = await self._tasks.submit(spec)
                status.results[name] = await self._tasks.result(
                    task_id, timeout=spec.timeout_seconds * spec.max_attempts + 5
                )
                status.node_states[name] = "succeeded"
            except Exception as e:
                status.node_states[name] = "failed"
                status.failed = True
                failed_nodes.add(name)

        while remaining:
            ready = [
                name
                for name in remaining
                if all(
                    status.node_states.get(d) == "succeeded"
                    for d in dag.dependencies.get(name, ())
                )
            ]
            blocked_by_failure = [
                name
                for name in remaining
                if any(
                    status.node_states.get(d) in ("failed", "skipped")
                    for d in dag.dependencies.get(name, ())
                )
            ]
            for name in blocked_by_failure:
                status.node_states[name] = "skipped"
                remaining.pop(name)
            if not ready:
                if not blocked_by_failure and remaining:
                    # ни готовых, ни заблокированных сбоем — цикл зависимостей
                    for name in list(remaining):
                        status.node_states[name] = "failed"
                        remaining.pop(name)
                    status.failed = True
                continue
            await asyncio.gather(
                *(run_node(name, remaining.pop(name)) for name in ready)
            )
        status.done = True
        if self._events:
            await self._events.publish(
                Event(
                    type="scheduler.dag.completed" if not status.failed else "scheduler.dag.failed",
                    payload={"dag_id": status.dag_id, "nodes": status.node_states},
                )
            )


class SchedulerRuntime(RuntimeModule):
    def __init__(self) -> None:
        self._service: SchedulerService | None = None

    def manifest(self) -> ModuleManifest:
        return ModuleManifest(
            id="scheduler-runtime",
            version="0.2.0",
            provides_ports=("SchedulerPort@1",),
            requires_ports=("TaskPort@1", "EventPort@1"),
            provides_events=(
                "scheduler.timer.fired@1",
                "scheduler.dag.completed@1",
                "scheduler.dag.failed@1",
            ),
        )

    async def init(self, ctx: ModuleContext) -> None:
        self._service = SchedulerService(ctx.port("TaskPort@1"), ctx.port("EventPort@1"))
        ctx.register("SchedulerPort@1", self._service)

    async def start(self) -> None:
        assert self._service
        await self._service.start()

    async def stop(self, deadline_seconds: float = 10.0) -> None:
        if self._service:
            await self._service.stop()
