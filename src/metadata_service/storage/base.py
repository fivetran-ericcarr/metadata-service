"""Storage interface and backend factory."""

from __future__ import annotations

from typing import Protocol, runtime_checkable

from ..config import Settings
from ..exceptions import StorageError


@runtime_checkable
class MetadataStorage(Protocol):
    """Snapshot storage contract.

    Implementations persist normalized metadata snapshots, maintain a pointer to
    ``latest``, and expose the previous snapshot for drift comparison.
    """

    def write_snapshot(self, metadata: dict, snapshot_name: str | None = None) -> str:
        """Write a snapshot (and update ``latest``). Returns the snapshot URI."""
        ...

    def read_latest(self) -> dict | None:
        ...

    def read_previous(self) -> dict | None:
        ...

    def list_snapshots(self) -> list[str]:
        ...


def get_storage(settings: Settings) -> MetadataStorage:
    """Return a storage backend based on ``METADATA_STORAGE_BACKEND``."""
    backend = (settings.metadata_storage_backend or "local").lower()
    if backend == "local":
        from .local_storage import LocalStorage

        return LocalStorage(settings.metadata_local_path,
                            retain=settings.metadata_retention_snapshots)
    if backend == "s3":
        from .s3_storage import S3Storage

        if not settings.metadata_s3_bucket:
            raise StorageError("METADATA_S3_BUCKET must be set when METADATA_STORAGE_BACKEND=s3.")
        return S3Storage(settings.metadata_s3_bucket, settings.metadata_s3_prefix,
                         retain=settings.metadata_retention_snapshots)
    raise StorageError(f"Unknown storage backend: {backend!r}")
