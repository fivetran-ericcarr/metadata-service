"""Shared HTTP helpers for the API clients."""

from __future__ import annotations


def parse_retry_after(value: str | None, default: float = 2.0, max_seconds: float = 60.0) -> float:
    """Parse a numeric ``Retry-After`` header, clamped to ``max_seconds`` so one
    header can't stall a run for hours. HTTP-date values fall back to ``default``."""
    if not value:
        return default
    try:
        return max(0.0, min(float(value), max_seconds))
    except (TypeError, ValueError):
        return default
