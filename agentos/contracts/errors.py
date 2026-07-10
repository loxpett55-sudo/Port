"""Единая модель ошибок платформы."""

from __future__ import annotations


class AgentOSError(Exception):
    """Базовая ошибка платформы.

    retryable: может ли вызывающая сторона повторить операцию.
    """

    retryable: bool = False

    def __init__(self, message: str, *, retryable: bool | None = None) -> None:
        super().__init__(message)
        if retryable is not None:
            self.retryable = retryable


class ContractError(AgentOSError):
    """Нарушение контракта: неверная схема, несовместимая версия."""


class PortNotFoundError(AgentOSError):
    """Запрошенный порт не зарегистрирован."""


class ModuleError(AgentOSError):
    """Ошибка жизненного цикла модуля."""


class DependencyCycleError(ModuleError):
    """Цикл в графе зависимостей модулей."""


class PermissionDeniedError(AgentOSError):
    """Действие запрещено политикой или отсутствием permission."""


class ResourceExhaustedError(AgentOSError):
    """Бюджет или квота ресурса исчерпаны."""

    retryable = True


class TransientError(AgentOSError):
    """Временный сбой: сеть, недоступность провайдера."""

    retryable = True
