"""Pluggable snapshot storage backends."""

from .base import MetadataStorage, get_storage
from .local_storage import LocalStorage

__all__ = ["MetadataStorage", "LocalStorage", "get_storage"]
