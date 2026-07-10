"""SDK AgentOS.

Python SDK — два уровня:
- embedded: `agentos.platform.AgentOSPlatform` — платформа в вашем процессе;
- client: `agentos.sdk.client.AgentOSClient` — HTTP-клиент к удалённой
  платформе (этот модуль).

JavaScript/TypeScript SDK генерируется из тех же контрактов (API First).
"""

from agentos.sdk.client import AgentOSClient

__all__ = ["AgentOSClient"]
