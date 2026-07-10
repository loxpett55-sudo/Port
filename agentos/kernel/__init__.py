"""Микроядро AgentOS: запуск, жизненный цикл модулей, DI, реестр
сервисов, маршрутизация сообщений, bootstrap-конфигурация.

Ядро не содержит бизнес-логики и зависит только от contracts.
"""

from agentos.kernel.kernel import Kernel
from agentos.kernel.registry import ServiceRegistry

__all__ = ["Kernel", "ServiceRegistry"]
