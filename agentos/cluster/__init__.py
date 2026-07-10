"""Кластерный режим (топологии T3/T4).

Три примитива, делающие платформу распределённой без изменения контрактов:

- ConsistentHashRing — шардирование акторов (агентов, сессий) по узлам;
- BusBridge — федерация Event Bus узлов поверх TCP (эталонный транспорт;
  продакшн-брокеры NATS/Kafka подключаются таким же адаптером);
- DistributedAgentGateway — прозрачная маршрутизация сообщений агенту
  на узел-владелец через события request/reply.
"""

from agentos.cluster.bridge import BusBridge
from agentos.cluster.gateway import DistributedAgentGateway
from agentos.cluster.hashring import ConsistentHashRing

__all__ = ["BusBridge", "ConsistentHashRing", "DistributedAgentGateway"]
