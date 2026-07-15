"""Lineage graph helper built from normalized dbt lineage edges."""

from __future__ import annotations

from collections import defaultdict


class LineageGraph:
    """Directed graph of ``from_unique_id -> to_unique_id`` edges."""

    def __init__(self, edges: list[dict] | None = None) -> None:
        self._children: dict[str, list[str]] = defaultdict(list)
        self._parents: dict[str, list[str]] = defaultdict(list)
        for edge in edges or []:
            frm = edge.get("from_unique_id")
            to = edge.get("to_unique_id")
            if frm and to:
                self._children[frm].append(to)
                self._parents[to].append(frm)

    def descendants(self, uid: str) -> list[str]:
        """All downstream nodes reachable from ``uid`` (BFS, excludes uid)."""
        return self._reach(uid, self._children)

    def ancestors(self, uid: str) -> list[str]:
        """All upstream nodes ``uid`` depends on (BFS, excludes uid)."""
        return self._reach(uid, self._parents)

    @staticmethod
    def _reach(uid: str, adjacency: dict[str, list[str]]) -> list[str]:
        seen: set[str] = set()
        queue = list(adjacency.get(uid, []))
        order: list[str] = []
        while queue:
            node = queue.pop(0)
            if node in seen:
                continue
            seen.add(node)
            order.append(node)
            queue.extend(adjacency.get(node, []))
        return order
