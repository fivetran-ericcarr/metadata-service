"""Root pytest conftest: guarantee test isolation from any real ``.env`` file.

``Settings`` resolves ``METADATA_SERVICE_ENV_FILE`` (default ``.env``, relative
to the process CWD) at class-definition time — see
``metadata_service/config.py`` — so a developer's own local ``.env`` (entirely
normal when testing a live warehouse integration, and never committed; it's
gitignored) would otherwise leak real credentials into any test that
constructs a bare ``Settings()``/``Settings(partial kwargs)``: fields not
explicitly passed fall through to whatever the real ``.env`` says, silently
turning a "no warehouse credentials configured" test case into one that IS
configured. "Tests use local fixtures, never live APIs/credentials" is this
repo's stated convention; this is what actually enforces it.

This must run before ``metadata_service.config`` is imported anywhere, which is
why it lives in a ROOT-level ``conftest.py`` rather than ``tests/conftest.py``:
pytest imports conftest.py files top-down (ancestor directories before
descendants, and before collecting any test module in them), so this always
wins the race regardless of which test path pytest starts with.
"""

from __future__ import annotations

import os

# A path that cannot exist as a real file; pydantic-settings silently skips
# loading when the configured env_file isn't found, exactly like the normal
# "no .env in this repo" case. setdefault() so an operator who deliberately
# sets METADATA_SERVICE_ENV_FILE (e.g. to test against a fixture .env) is
# still respected rather than overridden.
os.environ.setdefault(
    "METADATA_SERVICE_ENV_FILE",
    "/nonexistent/metadata-service-tests-must-not-load-a-real-env-file",
)
