"""Load a metadata-service snapshot from a file path or the REST API.

The snapshot is the contract (see the metadata-service ARTIFACTS.md): this module
is the ONLY place the middleware touches a transport. Everything downstream works
on the plain dict.
"""

from __future__ import annotations

import json
from pathlib import Path

import httpx

EXPECTED_VERSION = "1.0"


class SnapshotError(RuntimeError):
    """The snapshot could not be loaded or is not usable for gating."""


def load_snapshot(source: str, api_key: str | None = None, timeout: float = 30.0) -> dict:
    """Load a snapshot from ``source``.

    - ``http(s)://host:port`` or a full ``/metadata/latest`` URL → REST, sending
      ``X-API-Key`` when ``api_key`` is set (the service's METADATA_API_KEY).
    - anything else → a local ``latest.json`` path.
    """
    if source.startswith(("http://", "https://")):
        url = source if source.rstrip("/").endswith("/metadata/latest") \
            else source.rstrip("/") + "/metadata/latest"
        headers = {"X-API-Key": api_key} if api_key else {}
        try:
            resp = httpx.get(url, headers=headers, timeout=timeout)
        except httpx.HTTPError as exc:
            raise SnapshotError(f"Could not reach metadata-service at {url}: {exc}") from exc
        if resp.status_code == 401:
            raise SnapshotError("metadata-service rejected the API key (401). Set --api-key.")
        if resp.status_code != 200:
            raise SnapshotError(f"metadata-service returned {resp.status_code} for {url}.")
        doc = resp.json()
    else:
        path = Path(source)
        if not path.exists():
            raise SnapshotError(f"Snapshot file not found: {path}")
        try:
            doc = json.loads(path.read_text(encoding="utf-8"))
        except (OSError, json.JSONDecodeError) as exc:
            raise SnapshotError(f"Could not read snapshot {path}: {exc}") from exc

    if not isinstance(doc, dict) or "warehouse_objects" not in doc:
        raise SnapshotError("Payload does not look like a metadata-service snapshot "
                            "(missing warehouse_objects).")
    version = doc.get("version")
    if version != EXPECTED_VERSION:
        # Contract-version awareness: newer fields degrade to defaults, so gate
        # decisions may be under-informed. Surface it rather than guess.
        raise SnapshotError(
            f"Snapshot schema version {version!r} != supported {EXPECTED_VERSION!r}; "
            "refusing to gate on a contract this middleware was not built against."
        )
    return doc
