"""Authentication for the remote (Streamable HTTP) MCP server.

Two modes, chosen by ``MCP_AUTH`` (see :func:`resolve_auth_mode`):

``api_key``
    A static bearer key (``METADATA_API_KEY`` plus any ``MCP_API_KEYS``) in
    ``Authorization: Bearer <key>`` or ``X-API-Key``. Enforced by
    :class:`ApiKeyMiddleware`. Works for Claude Code, Claude custom connectors
    (Request headers), Google ADK / Vertex AI agents (``headers=``), Databricks
    UC HTTP connections (bearer token) and any SDK client.

``oauth``
    OAuth 2.1 resource server (MCP authorization spec): the server publishes
    RFC 9728 protected-resource metadata pointing at an external authorization
    server (``MCP_OAUTH_ISSUER``) and validates its JWT access tokens against
    the issuer's JWKS (:class:`JWTVerifier`). Required by ChatGPT (OAuth or no
    auth only) and Snowflake Cortex external MCP servers (OAuth only); also the
    "Sign in" path of Claude connectors. The authorization server must support
    Dynamic Client Registration or Client ID Metadata Documents. Static keys are
    accepted too, so one deployment serves both kinds of client.
"""

from __future__ import annotations

import json
import logging
import secrets

from mcp.server.auth.provider import AccessToken

logger = logging.getLogger(__name__)

#: Paths that never require auth (health checks, OAuth discovery).
PUBLIC_PATHS = ("/health", "/.well-known/")


def resolve_auth_mode(settings) -> str:
    mode = (settings.mcp_auth or "auto").strip().lower()
    if mode != "auto":
        return mode
    if settings.mcp_oauth_issuer:
        return "oauth"
    if settings.mcp_bearer_keys():
        return "api_key"
    return "none"


def _key_matches(provided: str | None, keys: list[str]) -> int | None:
    """Index of the matching key (constant time per key), else None."""
    if not provided:
        return None
    raw = provided.encode("utf-8", "surrogateescape")
    match = None
    for i, k in enumerate(keys):
        if secrets.compare_digest(raw, k.encode("utf-8")):
            match = i
    return match


class StaticKeyVerifier:
    """TokenVerifier accepting the configured static keys."""

    def __init__(self, keys: list[str]) -> None:
        self._keys = list(keys)

    async def verify_token(self, token: str) -> AccessToken | None:
        idx = _key_matches(token, self._keys)
        if idx is None:
            return None
        return AccessToken(token="api-key", client_id=f"api-key-{idx}", scopes=["metadata:read"])


class JWTVerifier:
    """Validates JWT access tokens from an external OAuth authorization server.

    Checks signature (issuer JWKS), ``iss``, ``exp``/``nbf``, ``aud`` (when
    ``MCP_OAUTH_AUDIENCE`` is set — set it to the resource URL or API identifier
    your authorization server puts in ``aud``) and required scopes.
    """

    def __init__(self, issuer: str, *, audience: str | None = None, jwks_url: str | None = None,
                 required_scopes: list[str] | None = None, leeway: int = 30) -> None:
        self.issuer = issuer.rstrip("/")
        self.audience = audience
        self._jwks_url = jwks_url
        self.required_scopes = required_scopes or []
        self.leeway = leeway
        self._jwk_client = None

    def _discover_jwks_url(self) -> str:
        import httpx

        for suffix in ("/.well-known/openid-configuration", "/.well-known/oauth-authorization-server"):
            try:
                resp = httpx.get(self.issuer + suffix, timeout=10)
                if resp.status_code == 200 and resp.json().get("jwks_uri"):
                    return resp.json()["jwks_uri"]
            except Exception:  # try the next discovery document
                continue
        raise RuntimeError("Could not discover jwks_uri from MCP_OAUTH_ISSUER; set MCP_OAUTH_JWKS_URL.")

    def _client(self):
        if self._jwk_client is None:
            import jwt

            self._jwk_client = jwt.PyJWKClient(self._jwks_url or self._discover_jwks_url(),
                                               cache_keys=True, lifespan=3600)
        return self._jwk_client

    def decode(self, token: str) -> dict:
        import jwt

        key = self._client().get_signing_key_from_jwt(token).key
        options = {"require": ["exp", "iss"], "verify_aud": bool(self.audience)}
        claims = jwt.decode(token, key, algorithms=["RS256", "RS384", "RS512", "ES256", "ES384", "PS256"],
                            audience=self.audience, options=options, leeway=self.leeway)
        if (claims.get("iss") or "").rstrip("/") != self.issuer:
            raise jwt.InvalidIssuerError("issuer mismatch")
        return claims

    async def verify_token(self, token: str) -> AccessToken | None:
        import anyio

        try:
            claims = await anyio.to_thread.run_sync(lambda: self.decode(token))
        except Exception as exc:
            logger.info("Rejected OAuth token: %s", type(exc).__name__)
            return None
        scope = claims.get("scope") or claims.get("scp") or []
        scopes = scope.split() if isinstance(scope, str) else list(scope)
        if any(s not in scopes for s in self.required_scopes):
            logger.info("Rejected OAuth token: missing required scope")
            return None
        return AccessToken(token=token, client_id=str(claims.get("client_id") or claims.get("azp") or
                                                      claims.get("sub") or "oauth"),
                           scopes=scopes, expires_at=claims.get("exp"), subject=claims.get("sub"))


class CompositeVerifier:
    """Accept a static key first, else an OAuth JWT."""

    def __init__(self, *verifiers) -> None:
        self._verifiers = [v for v in verifiers if v is not None]

    async def verify_token(self, token: str) -> AccessToken | None:
        for v in self._verifiers:
            result = await v.verify_token(token)
            if result is not None:
                return result
        return None


class ApiKeyMiddleware:
    """ASGI middleware enforcing a static bearer key on every non-public path."""

    def __init__(self, app, keys: list[str]) -> None:
        self.app = app
        self._keys = list(keys)

    async def __call__(self, scope, receive, send):
        if scope["type"] != "http" or scope.get("path", "").startswith(PUBLIC_PATHS):
            return await self.app(scope, receive, send)
        headers = {k.decode("latin-1").lower(): v.decode("latin-1") for k, v in scope.get("headers") or []}
        provided = headers.get("x-api-key")
        auth = headers.get("authorization") or ""
        if not provided and auth.lower().startswith("bearer "):
            provided = auth[7:].strip()
        if _key_matches(provided, self._keys) is None:
            body = json.dumps({"error": "invalid_token",
                               "error_description": "Missing or invalid API key."}).encode()
            await send({"type": "http.response.start", "status": 401,
                        "headers": [(b"content-type", b"application/json"),
                                    (b"www-authenticate", b'Bearer error="invalid_token"')]})
            await send({"type": "http.response.body", "body": body})
            return
        await self.app(scope, receive, send)

