"""S3 snapshot storage (optional; requires ``boto3``).

Layout::

    s3://bucket/prefix/latest.json
    s3://bucket/prefix/2026/06/25/2026-06-25T12-00-00Z.json
"""

from __future__ import annotations

import json
import logging
import re

from ..exceptions import StorageError
from ..models.common import snapshot_timestamp

logger = logging.getLogger(__name__)

_LATEST = "latest.json"
_SNAPSHOT_NAME = re.compile(r"^\d{4}-\d{2}-\d{2}T\d{2}-\d{2}-\d{2}Z\.json$")


class S3Storage:
    def __init__(self, bucket: str, prefix: str = "metadata", *, client=None,
                 retain: int | None = None) -> None:
        self._bucket = bucket
        self._prefix = prefix.strip("/")
        self._retain = retain if retain and retain > 0 else None
        if client is not None:
            self._s3 = client
        else:
            try:
                import boto3  # noqa: PLC0415
            except ImportError as exc:  # pragma: no cover - optional dependency
                raise StorageError(
                    "S3 storage requires boto3. Install with: pip install 'metadata-service[s3]'."
                ) from exc
            self._s3 = boto3.client("s3")

    def _key(self, *parts: str) -> str:
        return "/".join([p for p in (self._prefix, *parts) if p])

    def write_snapshot(self, metadata: dict, snapshot_name: str | None = None,
                       *, update_latest: bool = True) -> str:
        name = snapshot_name or snapshot_timestamp()
        generated_at = metadata.get("generated_at") or name
        date_part = generated_at[:10]
        try:
            year, month, day = date_part.split("-")
        except ValueError:
            year = month = day = "unknown"

        body = json.dumps(metadata, indent=2, default=str).encode("utf-8")
        snapshot_key = self._key(year, month, day, f"{name}.json")
        try:
            self._s3.put_object(Bucket=self._bucket, Key=snapshot_key, Body=body,
                                ContentType="application/json")
            if update_latest:
                self._s3.put_object(Bucket=self._bucket, Key=self._key(_LATEST), Body=body,
                                    ContentType="application/json")
        except Exception as exc:  # boto ClientError etc.
            raise StorageError(f"Failed to write snapshot to s3://{self._bucket}/{snapshot_key}: {exc}") from exc

        self._prune()
        uri = f"s3://{self._bucket}/{snapshot_key}"
        logger.info("Wrote snapshot: %s", uri)
        return uri

    def read_latest(self) -> dict | None:
        return self._read(self._key(_LATEST))

    def read_previous(self) -> dict | None:
        keys = self._snapshot_keys()
        if len(keys) < 2:
            return None
        return self._read(keys[-2])

    def list_snapshots(self) -> list[str]:
        return [f"s3://{self._bucket}/{k}" for k in self._snapshot_keys()]

    # -- internals --------------------------------------------------------
    def _snapshot_keys(self) -> list[str]:
        keys: list[str] = []
        paginator = self._s3.get_paginator("list_objects_v2")
        for page in paginator.paginate(Bucket=self._bucket, Prefix=self._prefix):
            for obj in page.get("Contents", []) or []:
                key = obj["Key"]
                if _SNAPSHOT_NAME.match(key.rsplit("/", 1)[-1]):
                    keys.append(key)
        return sorted(keys, key=lambda k: k.rsplit("/", 1)[-1])

    def _prune(self) -> None:
        """Delete the oldest timestamped snapshots beyond the retention count."""
        if self._retain is None:
            return
        keys = self._snapshot_keys()
        for old in keys[: max(0, len(keys) - self._retain)]:
            try:
                self._s3.delete_object(Bucket=self._bucket, Key=old)
                logger.info("Pruned old snapshot: s3://%s/%s", self._bucket, old)
            except Exception as exc:  # never fail a build over cleanup
                logger.warning("Could not prune snapshot %s: %s", old, exc)

    def _read(self, key: str) -> dict | None:
        try:
            resp = self._s3.get_object(Bucket=self._bucket, Key=key)
            return json.loads(resp["Body"].read())
        except self._s3.exceptions.NoSuchKey:  # type: ignore[attr-defined]
            return None
        except Exception as exc:
            raise StorageError(f"Failed to read snapshot s3://{self._bucket}/{key}: {exc}") from exc
