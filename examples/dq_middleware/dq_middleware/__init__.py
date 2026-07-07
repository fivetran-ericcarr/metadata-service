"""Toy DQ policy middleware — an example consumer of the metadata-service contract.

The metadata-service produces *facts* (the normalized snapshot). This middleware
turns those facts into *organizational decisions*: pass/fail policy verdicts a
CI pipeline can gate on, allow/deny answers an orchestrator can ask before
triggering a reverse-ETL sync, and a waiver mechanism so exceptions are explicit,
attributed, and expiring instead of silent.

Functionally complete, intentionally small. See README.md for the tour.
"""

__version__ = "0.1.0"
