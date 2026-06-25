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

    def children(self, uid: str) -> list[str]:
        return list(self._children.get(uid, []))

    def parents(self, uid: str) -> list[str]:
        return list(self._parents.get(uid, []))

    def descendants(self, uid: str) -> list[str]:
        """All downstream nodes reachable from ``uid`` (BFS, excludes uid)."""
        seen: set[str] = set()
        queue = list(self._children.get(uid, []))
        order: list[str] = []
        while queue:
            node = queue.pop(0)
            if node in seen:
                continue
            seen.add(node)
            order.append(node)
            queue.extend(self._children.get(node, []))
        return order
