"""Тесты микроядра: реестр, граф зависимостей, жизненный цикл."""

import pytest

from agentos.contracts.errors import DependencyCycleError, ModuleError, PortNotFoundError
from agentos.contracts.module import ModuleContext, ModuleManifest, ModuleState, RuntimeModule
from agentos.kernel import Kernel, ServiceRegistry
from agentos.kernel.module_host import DependencyGraph


class StubModule(RuntimeModule):
    def __init__(self, id, provides=(), requires=(), journal=None):
        self._m = ModuleManifest(
            id=id, version="1.0.0", provides_ports=tuple(provides), requires_ports=tuple(requires)
        )
        self.journal = journal if journal is not None else []

    def manifest(self):
        return self._m

    async def init(self, ctx: ModuleContext):
        for p in self._m.provides_ports:
            ctx.register(p, object())
        self.journal.append(("init", self._m.id))

    async def start(self):
        self.journal.append(("start", self._m.id))

    async def stop(self, deadline_seconds=10.0):
        self.journal.append(("stop", self._m.id))


def test_registry_resolve_and_replace():
    reg = ServiceRegistry()
    reg.register("FooPort@1", "impl-a", owner="mod-a")
    assert reg.resolve("FooPort@1") == "impl-a"

    seen = []
    reg.watch("FooPort@1", seen.append)
    reg.register("FooPort@1", "impl-b", owner="mod-b")  # hot-swap
    assert reg.resolve("FooPort@1") == "impl-b"
    assert seen == ["impl-b"]

    with pytest.raises(PortNotFoundError):
        reg.resolve("MissingPort@1")


def test_dependency_order_and_cycle():
    a = StubModule("a", provides=["A@1"])
    b = StubModule("b", provides=["B@1"], requires=["A@1"])
    c = StubModule("c", requires=["B@1", "A@1"])
    order = [m.manifest().id for m in DependencyGraph.order([c, b, a])]
    assert order.index("a") < order.index("b") < order.index("c")

    x = StubModule("x", provides=["X@1"], requires=["Y@1"])
    y = StubModule("y", provides=["Y@1"], requires=["X@1"])
    with pytest.raises(DependencyCycleError):
        DependencyGraph.order([x, y])


async def test_kernel_boot_and_shutdown_order():
    journal = []
    a = StubModule("a", provides=["A@1"], journal=journal)
    b = StubModule("b", requires=["A@1"], journal=journal)
    kernel = Kernel()
    await kernel.boot([b, a])
    assert kernel.host.state("a") is ModuleState.RUNNING
    await kernel.shutdown()
    # запуск: a раньше b; остановка — в обратном порядке
    assert journal == [
        ("init", "a"), ("start", "a"),
        ("init", "b"), ("start", "b"),
        ("stop", "b"), ("stop", "a"),
    ]


async def test_kernel_missing_port_fails():
    kernel = Kernel()
    with pytest.raises(ModuleError, match="не удовлетворены порты"):
        await kernel.boot([StubModule("lonely", requires=["Nowhere@1"])])
