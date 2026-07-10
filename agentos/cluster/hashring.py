"""Consistent hashing — размещение акторов по узлам кластера.

Виртуальные узлы сглаживают распределение; при добавлении/удалении узла
переезжает только ~1/N ключей (минимальная миграция акторов).
"""

from __future__ import annotations

import bisect
import hashlib


def _hash(value: str) -> int:
    return int(hashlib.md5(value.encode()).hexdigest()[:16], 16)


class ConsistentHashRing:
    def __init__(self, *, virtual_nodes: int = 128) -> None:
        self._virtual = virtual_nodes
        self._ring: list[tuple[int, str]] = []
        self._nodes: set[str] = set()

    @property
    def nodes(self) -> set[str]:
        return set(self._nodes)

    def add_node(self, node_id: str) -> None:
        if node_id in self._nodes:
            return
        self._nodes.add(node_id)
        for i in range(self._virtual):
            bisect.insort(self._ring, (_hash(f"{node_id}#{i}"), node_id))

    def remove_node(self, node_id: str) -> None:
        self._nodes.discard(node_id)
        self._ring = [(h, n) for h, n in self._ring if n != node_id]

    def owner(self, key: str) -> str:
        if not self._ring:
            raise LookupError("в кольце нет узлов")
        h = _hash(key)
        index = bisect.bisect_right(self._ring, (h, "￿")) % len(self._ring)
        return self._ring[index][1]
