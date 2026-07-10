"""Storage Runtime — единая точка доступа к хранилищам.

Регистрирует порты KVStorePort/EventLogPort/VectorStorePort.
Бэкенд выбирается конфигурацией: memory (по умолчанию) или sqlite
(durable, профиль T1/T2). Продакшн-бэкенды — адаптеры-плагины.
"""

from __future__ import annotations

from pathlib import Path

from agentos.contracts.module import ModuleContext, ModuleManifest, RuntimeModule


class StorageRuntime(RuntimeModule):
    def manifest(self) -> ModuleManifest:
        return ModuleManifest(
            id="storage-runtime",
            version="0.2.0",
            provides_ports=("KVStorePort@1", "EventLogPort@1", "VectorStorePort@1"),
            config_schema={
                "backend": {"type": "string", "enum": ["memory", "sqlite"]},
                "path": {"type": "string"},
            },
        )

    async def init(self, ctx: ModuleContext) -> None:
        backend = ctx.config.get("backend", "memory")
        from agentos.adapters.memory_store import (
            InMemoryEventLog,
            InMemoryKV,
            InMemoryVectorStore,
        )

        if backend == "sqlite":
            from agentos.adapters.sqlite_store import SqliteEventLog, SqliteKV

            data_dir = Path(ctx.config.get("path", "./data"))
            data_dir.mkdir(parents=True, exist_ok=True)
            ctx.register("KVStorePort@1", SqliteKV(data_dir / "kv.db"))
            ctx.register("EventLogPort@1", SqliteEventLog(data_dir / "events.db"))
        else:
            ctx.register("KVStorePort@1", InMemoryKV())
            ctx.register("EventLogPort@1", InMemoryEventLog())
        # Векторный индекс: встроенный (эталонный); sqlite-вариант не даёт
        # преимуществ для линейного поиска, durable-вариант — плагином.
        ctx.register("VectorStorePort@1", InMemoryVectorStore())
