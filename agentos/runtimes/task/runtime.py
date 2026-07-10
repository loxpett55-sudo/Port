"""Task Runtime — исполнение атомарных задач: пул воркеров с приоритетной
очередью, ретраи с экспоненциальной задержкой, dead-letter, идемпотентность
по ключу, события жизненного цикла."""

from __future__ import annotations

import asyncio
import heapq
import itertools
import logging

from agentos.contracts.errors import AgentOSError
from agentos.contracts.events import Event, EventPort
from agentos.contracts.module import ModuleContext, ModuleManifest, RuntimeModule
from agentos.contracts.task import TaskHandler, TaskPort, TaskRecord, TaskSpec, TaskState

log = logging.getLogger("agentos.task-runtime")


class TaskService(TaskPort):
    def __init__(
        self,
        events: EventPort | None = None,
        *,
        workers: int = 4,
        base_backoff: float = 0.02,
    ) -> None:
        self._events = events
        self._handlers: dict[str, TaskHandler] = {}
        self._records: dict[str, TaskRecord] = {}
        self._by_idempotency: dict[str, str] = {}
        self._queue: list[tuple[int, int, str]] = []
        self._seq = itertools.count()
        self._wakeup = asyncio.Event()
        self._done: dict[str, asyncio.Event] = {}
        self._workers: list[asyncio.Task] = []
        self._n_workers = workers
        self._base_backoff = base_backoff
        self._running = False

    # --- жизненный цикл ----------------------------------------------------

    async def start(self) -> None:
        self._running = True
        self._workers = [
            asyncio.create_task(self._worker_loop()) for _ in range(self._n_workers)
        ]

    async def stop(self) -> None:
        self._running = False
        self._wakeup.set()
        for w in self._workers:
            w.cancel()
        for w in self._workers:
            try:
                await w
            except asyncio.CancelledError:
                pass

    # --- TaskPort ------------------------------------------------------------

    def register_handler(self, task_type: str, handler: TaskHandler) -> None:
        self._handlers[task_type] = handler

    async def submit(self, spec: TaskSpec) -> str:
        if spec.idempotency_key and spec.idempotency_key in self._by_idempotency:
            return self._by_idempotency[spec.idempotency_key]
        record = TaskRecord(spec=spec)
        self._records[spec.id] = record
        self._done[spec.id] = asyncio.Event()
        if spec.idempotency_key:
            self._by_idempotency[spec.idempotency_key] = spec.id
        heapq.heappush(self._queue, (int(spec.priority), next(self._seq), spec.id))
        self._wakeup.set()
        await self._emit("task.submitted", record)
        return spec.id

    async def get(self, task_id: str) -> TaskRecord:
        try:
            return self._records[task_id]
        except KeyError:
            raise AgentOSError(f"задача не найдена: {task_id}") from None

    async def cancel(self, task_id: str) -> bool:
        record = await self.get(task_id)
        if record.state in (TaskState.PENDING, TaskState.RETRYING):
            self._finish(record, TaskState.CANCELLED)
            await self._emit("task.cancelled", record)
            return True
        return False

    async def result(self, task_id: str, timeout: float = 60.0):
        await asyncio.wait_for(self._done[task_id].wait(), timeout)
        record = self._records[task_id]
        if record.state is TaskState.SUCCEEDED:
            return record.result
        raise AgentOSError(f"задача {task_id}: {record.state.value}: {record.error}")

    # --- воркеры ----------------------------------------------------------------

    async def _worker_loop(self) -> None:
        while self._running:
            if not self._queue:
                self._wakeup.clear()
                await self._wakeup.wait()
                continue
            _, _, task_id = heapq.heappop(self._queue)
            record = self._records[task_id]
            if record.state in (TaskState.CANCELLED, TaskState.DEAD_LETTERED):
                continue
            await self._execute(record)

    async def _execute(self, record: TaskRecord) -> None:
        spec = record.spec
        handler = self._handlers.get(spec.type)
        if handler is None:
            record.error = f"нет обработчика для типа '{spec.type}'"
            self._finish(record, TaskState.DEAD_LETTERED)
            await self._emit("task.deadlettered", record)
            return

        record.state = TaskState.RUNNING
        record.attempts += 1
        await self._emit("task.started", record)
        try:
            record.result = await asyncio.wait_for(
                handler(spec.input), timeout=spec.timeout_seconds
            )
            self._finish(record, TaskState.SUCCEEDED)
            await self._emit("task.completed", record)
        except Exception as e:
            record.error = str(e) or type(e).__name__
            if record.attempts < spec.max_attempts:
                record.state = TaskState.RETRYING
                await self._emit("task.retrying", record)
                delay = self._base_backoff * (2 ** (record.attempts - 1))
                asyncio.get_running_loop().call_later(delay, self._requeue, spec)
            else:
                self._finish(record, TaskState.DEAD_LETTERED)
                await self._emit("task.deadlettered", record)

    def _requeue(self, spec: TaskSpec) -> None:
        record = self._records.get(spec.id)
        if record and record.state is TaskState.RETRYING:
            heapq.heappush(self._queue, (int(spec.priority), next(self._seq), spec.id))
            self._wakeup.set()

    def _finish(self, record: TaskRecord, state: TaskState) -> None:
        record.state = state
        self._done[record.spec.id].set()

    async def _emit(self, type_: str, record: TaskRecord) -> None:
        if self._events:
            await self._events.publish(
                Event(
                    type=type_,
                    payload={
                        "task_type": record.spec.type,
                        "attempts": record.attempts,
                        "error": record.error,
                    },
                    subject=f"task:{record.spec.id}",
                    priority=record.spec.priority,
                )
            )


class TaskRuntime(RuntimeModule):
    def __init__(self) -> None:
        self._service: TaskService | None = None

    def manifest(self) -> ModuleManifest:
        return ModuleManifest(
            id="task-runtime",
            version="0.2.0",
            provides_ports=("TaskPort@1",),
            requires_ports=("EventPort@1",),
            provides_events=(
                "task.submitted@1", "task.started@1", "task.completed@1",
                "task.retrying@1", "task.cancelled@1", "task.deadlettered@1",
            ),
            config_schema={"workers": {"type": "integer", "minimum": 1}},
        )

    async def init(self, ctx: ModuleContext) -> None:
        self._service = TaskService(
            ctx.port("EventPort@1"), workers=int(ctx.config.get("workers", 4))
        )
        ctx.register("TaskPort@1", self._service)

    async def start(self) -> None:
        assert self._service
        await self._service.start()

    async def stop(self, deadline_seconds: float = 10.0) -> None:
        if self._service:
            await self._service.stop()

    @property
    def service(self) -> TaskService:
        assert self._service
        return self._service
