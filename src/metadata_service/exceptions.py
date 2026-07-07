"""Typed exceptions for the metadata service.

Errors are never silently swallowed. Extractors capture per-item failures into
an ``errors`` array and continue; fatal/config errors raise these exceptions.
"""

from __future__ import annotations


class MetadataServiceError(Exception):
    """Base class for all metadata-service errors."""


# --- Fivetran ------------------------------------------------------------
class FivetranError(MetadataServiceError):
    """Base class for Fivetran client errors."""


class FivetranAuthError(FivetranError):
    """Authentication failed (HTTP 401)."""


class FivetranPermissionError(FivetranError):
    """Authenticated but not permitted (HTTP 403)."""


class FivetranNotFoundError(FivetranError):
    """Requested resource not found (HTTP 404)."""


class FivetranRateLimitError(FivetranError):
    """Rate limit retries exhausted (HTTP 429)."""


# --- dbt ------------------------------------------------------------------
class DbtError(MetadataServiceError):
    """Base class for dbt client errors."""


class DbtAuthError(DbtError):
    """Authentication failed (HTTP 401)."""


class DbtPermissionError(DbtError):
    """Authenticated but not permitted (HTTP 403)."""


class DbtNotFoundError(DbtError):
    """Requested resource not found (HTTP 404)."""


class DbtArtifactNotFoundError(DbtNotFoundError):
    """Requested run artifact not found."""


class DbtRateLimitError(DbtError):
    """Rate limit retries exhausted (HTTP 429)."""


# --- Activations (reverse ETL / Census) ----------------------------------
class ActivationsError(MetadataServiceError):
    """Base class for Fivetran Activations (Census) client errors."""


class ActivationsAuthError(ActivationsError):
    """Authentication failed (HTTP 401)."""


class ActivationsRateLimitError(ActivationsError):
    """Rate limit retries exhausted (HTTP 429)."""


# --- Pipeline -------------------------------------------------------------
class StorageError(MetadataServiceError):
    """Storage backend read/write failure."""


class RefreshInProgressError(MetadataServiceError):
    """A snapshot build/refresh is already running in this process."""


class NormalizationError(MetadataServiceError):
    """Failed to normalize raw metadata into the canonical shape."""
