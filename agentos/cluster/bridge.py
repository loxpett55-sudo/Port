"""BusBridge — федерация Event Bus узлов поверх TCP.

Эталонный кластерный транспорт: события локальной шины пересылаются
пирами (newline-delimited JSON), защита от эха — по id события.
Продакшн-вариант (брокер NATS/Kafka-класса) реализует тот же интерфейс.
"""

from __future__ import annotations

import asyncio
import json
import logging
from collections import OrderedDict
from dataclasses import asdict

from agentos.contracts.events import Event, EventPort, Priority

log = logging.getLogger("agentos.cluster.bridge")


class _SeenIds:
    """LRU-набор id событий: не пересылать своё эхо и дубликаты."""

    def __init__(self, capacity: int = 10_000) -> None:
        self._data: OrderedDict[str, None] = OrderedDict()
        self._capacity = capacity

    def add(self, event_id: str) -> None:
        self._data[event_id] = None
        self._data.move_to_end(event_id)
        while len(self._data) > self._capacity:
            self._data.popitem(last=False)

    def __contains__(self, event_id: str) -> bool:
        return event_id in self._data


def _encode(node_id: str, event: Event) -> bytes:
    data = asdict(event)
    data["priority"] = int(event.priority)
    return (json.dumps({"origin": node_id, "event": data}, ensure_ascii=False) + "\n").encode()


def _decode(line: bytes) -> tuple[str, Event]:
    frame = json.loads(line)
    data = frame["event"]
    data["priority"] = Priority(data.get("priority", 1))
    return frame["origin"], Event(**data)


class BusBridge:
    def __init__(self, bus: EventPort, node_id: str) -> None:
        self._bus = bus
        self.node_id = node_id
        self._seen = _SeenIds()
        self._peers: list[asyncio.StreamWriter] = []
        self._server: asyncio.Server | None = None
        self._readers: list[asyncio.Task] = []
        self.port: int = 0

    async def start(self, host: str = "127.0.0.1", port: int = 0) -> None:
        """Поднять приём соединений от пиров и пересылку локальных событий."""
        self._server = await asyncio.start_server(self._accept, host, port)
        self.port = self._server.sockets[0].getsockname()[1]
        self._bus.subscribe("**", self._forward)

    async def connect(self, host: str, port: int) -> None:
        reader, writer = await asyncio.open_connection(host, port)
        self._peers.append(writer)
        self._readers.append(asyncio.create_task(self._read_loop(reader)))

    async def stop(self) -> None:
        for task in self._readers:
            task.cancel()
        for task in self._readers:
            try:
                await task
            except asyncio.CancelledError:
                pass
        for writer in self._peers:
            writer.close()
        if self._server:
            self._server.close()
            await self._server.wait_closed()

    # --- внутреннее ----------------------------------------------------------

    async def _accept(self, reader: asyncio.StreamReader, writer: asyncio.StreamWriter):
        self._peers.append(writer)
        self._readers.append(asyncio.create_task(self._read_loop(reader)))

    async def _forward(self, event: Event) -> None:
        """Локальное событие → всем пирам (кроме пришедших извне)."""
        if event.id in self._seen:
            return
        self._seen.add(event.id)
        frame = _encode(self.node_id, event)
        for writer in list(self._peers):
            try:
                writer.write(frame)
                await writer.drain()
            except (ConnectionError, RuntimeError):
                self._peers.remove(writer)

    async def _read_loop(self, reader: asyncio.StreamReader) -> None:
        while True:
            line = await reader.readline()
            if not line:
                return
            try:
                origin, event = _decode(line)
            except (json.JSONDecodeError, KeyError, ValueError):
                log.warning("bridge %s: повреждённый кадр от пира", self.node_id)
                continue
            if event.id in self._seen or origin == self.node_id:
                continue
            self._seen.add(event.id)
            await self._bus.publish(event)
