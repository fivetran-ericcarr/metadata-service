"""Local filesystem snapshot storage.

Layout::

    metadata_snapshots/
      latest.json
      2026/06/25/2026-06-25T12-00-00Z.json
"""

from __future__ import annotations

import json
import logging
from pathlib import Path

from ..exceptions import StorageError
from ..models.common import snapshot_timestamp

logger = logging.getLogger(__name__)

_LATEST = "latest.json"


class LocalStorage:
    def __init__(self, root: str) -> None:
        self._root = Path(root)
        self._root.mkdir(parents=True, exist_ok=True)

    def write_snapshot(self, metadata: dict, snapshot_name: str | None = None) -> str:
        name = snapshot_name or snapshot_timestamp()
        # Partition by the embedded generated_at date when available.
        generated_at = metadata.get("generated_at") or name
        date_part = generated_at[:10]  # YYYY-MM-DD
        try:
            year, month, day = date_part.split("-")
        except ValueError:
            year = month = day = "unknown"

        snapshot_dir = self._root / year / month / day
        snapshot_dir.mkdir(parents=True, exist_ok=True)
        snapshot_path = snapshot_dir / f"{name}.json"

        try:
            payload = json.dumps(metadata, indent=2, default=str)
            snapshot_path.write_text(payload, encoding="utf-8")
            (self._root / _LATEST).write_text(payload, encoding="utf-8")
        except OSError as exc:
            raise StorageError(f"Failed to write snapshot to {snapshot_path}: {exc}") from exc

        uri = str(snapshot_path.resolve())
        logger.info("Wrote snapshot: %s", uri)
        return uri

    def read_latest(self) -> dict | None:
        return self._read(self._root / _LATEST)

    def read_previous(self) -> dict | None:
        snapshots = self._snapshot_files()
        # snapshots[-1] is the newest timestamped file (== latest). Previous is [-2].
        if len(snapshots) < 2:
            return None
        return self._read(snapshots[-2])

    def list_snapshots(self) -> list[str]:
        return [str(p.resolve()) for p in self._snapshot_files()]

    # -- internals --------------------------------------------------------
    def _snapshot_files(self) -> list[Path]:
        files = [p for p in self._root.rglob("*.json") if p.name != _LATEST]
        return sorted(files, key=lambda p: p.name)

    @staticmethod
    def _read(path: Path) -> dict | None:
        if not path.exists():
            return None
        try:
            return json.loads(path.read_text(encoding="utf-8"))
        except (OSError, json.JSONDecodeError) as exc:
            raise StorageError(f"Failed to read snapshot {path}: {exc}") from exc
