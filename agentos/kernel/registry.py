"""Service Registry — реестр портов с версионированием и hot-swap."""

from __future__ import annotations

from typing import Any, Callable

from agentos.contracts.errors import PortNotFoundError
from agentos.contracts.module import PortRef


class ServiceRegistry:
    def __init__(self) -> None:
        self._providers: dict[str, Any] = {}
        self._owners: dict[str, str] = {}
        self._watchers: dict[str, list[Callable[[Any], None]]] = {}

    def register(self, spec: str, provider: Any, *, owner: str = "") -> None:
        """Зарегистрировать реализацию порта. Повторная регистрация того же
        порта — горячая замена: watchers уведомляются о новом провайдере."""
        key = PortRef.parse(spec).key()
        replacing = key in self._providers
        self._providers[key] = provider
        self._owners[key] = owner
        if replacing:
            for cb in self._watchers.get(key, []):
                cb(provider)

    def resolve(self, ref: PortRef | str) -> Any:
        key = ref.key() if isinstance(ref, PortRef) else PortRef.parse(ref).key()
        try:
            return self._providers[key]
        except KeyError:
            raise PortNotFoundError(f"порт не зарегистрирован: {key}") from None

    def has(self, spec: str) -> bool:
        return PortRef.parse(spec).key() in self._providers

    def unregister(self, spec: str) -> None:
        key = PortRef.parse(spec).key()
        self._providers.pop(key, None)
        self._owners.pop(key, None)

    def watch(self, spec: str, callback: Callable[[Any], None]) -> None:
        """Подписка на замену провайдера (для hot-swap потребителей)."""
        self._watchers.setdefault(PortRef.parse(spec).key(), []).append(callback)

    def list_ports(self) -> dict[str, str]:
        """port -> module id владельца (для Dashboard/Diagnostics)."""
        return dict(self._owners)
