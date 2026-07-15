"""Local filesystem snapshot storage.

Layout::

    metadata_snapshots/
      latest.json
      2026/06/25/2026-06-25T12-00-00Z.json
"""

from __future__ import annotations

import json
import logging
import os
import re
import tempfile
from pathlib import Path

from ..exceptions import StorageError
from ..models.common import snapshot_timestamp

logger = logging.getLogger(__name__)

_LATEST = "latest.json"
# Only files shaped like snapshot timestamps count as snapshots — a stray
# aliases.json or editor backup must not become drift's "previous" baseline.
# The optional -\d{6} tail is the microsecond component (older second-resolution
# names remain valid).
_SNAPSHOT_NAME = re.compile(r"^\d{4}-\d{2}-\d{2}T\d{2}-\d{2}-\d{2}(-\d{6})?Z\.json$")


def _fsync_dir(path: Path) -> None:
    """fsync a directory so a rename into it survives a crash. Best-effort:
    some platforms (e.g. Windows) can't fsync a directory handle."""
    try:
        dir_fd = os.open(str(path), os.O_RDONLY)
    except OSError:  # pragma: no cover - platform dependent
        return
    try:
        os.fsync(dir_fd)
    except OSError:  # pragma: no cover
        pass
    finally:
        os.close(dir_fd)


def _write_atomic(path: Path, payload: str) -> None:
    """Write via a temp file + os.replace so readers never see a torn file.

    latest.json is read concurrently by the REST API and MCP server; a plain
    write_text truncates in place, so a crash or concurrent read mid-write
    would break every consumer at once. The temp file and its directory are
    fsync'd so a rename that survives a crash points at fully-written data
    (without the fsync the rename can be journaled before the data blocks,
    leaving a truncated/zero-length snapshot after power loss).
    """
    fd, tmp_name = tempfile.mkstemp(dir=path.parent, prefix=f".{path.name}.", suffix=".tmp")
    try:
        with os.fdopen(fd, "w", encoding="utf-8") as fh:
            fh.write(payload)
            fh.flush()
            os.fsync(fh.fileno())
        os.replace(tmp_name, path)
        _fsync_dir(path.parent)
    except BaseException:
        try:
            os.unlink(tmp_name)
        except OSError:
            pass
        raise


class LocalStorage:
    def __init__(self, root: str, *, retain: int | None = None) -> None:
        self._root = Path(root)
        self._root.mkdir(parents=True, exist_ok=True)
        self._retain = retain if retain and retain > 0 else None

    def write_snapshot(self, metadata: dict, snapshot_name: str | None = None,
                       *, update_latest: bool = True) -> str:
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
            _write_atomic(snapshot_path, payload)
            if update_latest:
                _write_atomic(self._root / _LATEST, payload)
        except OSError as exc:
            raise StorageError(f"Failed to write snapshot to {snapshot_path}: {exc}") from exc

        self._prune()
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
        files = [p for p in self._root.rglob("*.json") if _SNAPSHOT_NAME.match(p.name)]
        return sorted(files, key=lambda p: p.name)

    def _prune(self) -> None:
        """Delete the oldest timestamped snapshots beyond the retention count."""
        if self._retain is None:
            return
        files = self._snapshot_files()
        for old in files[: max(0, len(files) - self._retain)]:
            try:
                old.unlink()
                logger.info("Pruned old snapshot: %s", old)
            except OSError as exc:  # never fail a build over cleanup
                logger.warning("Could not prune snapshot %s: %s", old, exc)

    @staticmethod
    def _read(path: Path) -> dict | None:
        if not path.exists():
            return None
        try:
            return json.loads(path.read_text(encoding="utf-8"))
        except (OSError, json.JSONDecodeError) as exc:
            raise StorageError(f"Failed to read snapshot {path}: {exc}") from exc
