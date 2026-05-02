"""Auth + rate-limit middleware. Both opt-in via Settings — empty defaults
keep local dev frictionless; flip them on before exposing the API outside
trusted network.

Auth model: a single shared API key (Settings.api_key). When set, every
request to an API path must carry it as `X-API-Key: <key>`. Static-asset
GETs and a small public allowlist (`/health`, `/capabilities`, `/docs`,
`/openapi.json`, `/redoc`, `/auth/*`) bypass auth so the dashboard can
boot, prompt the operator for the key, and the API docs remain reachable
from a browser. The dashboard stores the key in sessionStorage and adds
the header to every fetch.

Rate limit: per-source-IP token bucket in memory. Refills at
`rate_limit_per_minute / 60` tokens/sec; bursts up to `rate_limit_per_minute`.
Single-process only — for multi-worker put a Redis-backed limiter in
front (slowapi or similar).
"""
from __future__ import annotations
import time
from collections import defaultdict
from threading import Lock

from fastapi import Request
from fastapi.responses import JSONResponse
from starlette.middleware.base import BaseHTTPMiddleware

from config import get_settings


# Paths that NEVER require auth — needed so the dashboard can boot, the
# health probe works, and OpenAPI docs stay reachable. Anything else under
# the API surface must present X-API-Key when Settings.api_key is set.
_PUBLIC_PATHS = {
    "/health", "/capabilities",
    "/docs", "/redoc", "/openapi.json",
    "/auth/login", "/auth/logout",
}
# Static assets — dashboard HTML/JS/CSS. The frontend mount is at "/" so
# anything that doesn't look like an API path is treated as a static file.
_API_PREFIXES = (
    "/use_cases", "/pipeline", "/ontology", "/query", "/nl",
    "/agents", "/usage", "/graph", "/schema", "/datasources",
)


def _is_api_path(path: str) -> bool:
    return any(path.startswith(p) for p in _API_PREFIXES)


def _is_public(path: str) -> bool:
    if path in _PUBLIC_PATHS:
        return True
    # Anything that isn't an API prefix is treated as a static asset
    # (frontend HTML / CSS / JS / favicon / etc.) — fully public.
    return not _is_api_path(path)


class APIKeyAuthMiddleware(BaseHTTPMiddleware):
    """Reject API requests missing the configured X-API-Key header.

    No-op when Settings.api_key is empty — local dev stays frictionless.
    Constant-time comparison via hmac.compare_digest defends against the
    (paranoid) timing-side-channel route to brute-forcing the key.
    """

    async def dispatch(self, request: Request, call_next):
        s = get_settings()
        configured = s.api_key.strip()
        if not configured:
            return await call_next(request)
        if _is_public(request.url.path):
            return await call_next(request)
        provided = request.headers.get("x-api-key", "")
        import hmac
        if not provided or not hmac.compare_digest(provided, configured):
            return JSONResponse(
                status_code=401,
                content={"detail": "Missing or invalid API key. Set the X-API-Key header."},
            )
        return await call_next(request)


class _TokenBucket:
    """Per-IP token bucket. Refills at `rate / 60` tokens per second up to
    a burst capacity of `rate` tokens. Thread-safe via a single coarse
    lock — fine for the request volumes a pilot deployment sees.
    """
    __slots__ = ("rate", "_buckets", "_lock")

    def __init__(self, rate_per_minute: int):
        self.rate = float(rate_per_minute)
        self._buckets: dict[str, tuple[float, float]] = {}
        self._lock = Lock()

    def consume(self, key: str, n: float = 1.0) -> tuple[bool, float]:
        """Try to consume `n` tokens. Returns (allowed, retry_after_seconds)."""
        if self.rate <= 0:
            return True, 0.0
        now = time.monotonic()
        refill_per_sec = self.rate / 60.0
        with self._lock:
            tokens, last = self._buckets.get(key, (self.rate, now))
            tokens = min(self.rate, tokens + (now - last) * refill_per_sec)
            if tokens >= n:
                self._buckets[key] = (tokens - n, now)
                return True, 0.0
            # Not enough — compute when the bucket will have `n` tokens.
            need = n - tokens
            retry = need / refill_per_sec
            self._buckets[key] = (tokens, now)
            return False, retry


_bucket: _TokenBucket | None = None


def _get_bucket() -> _TokenBucket:
    global _bucket
    s = get_settings()
    if _bucket is None or _bucket.rate != float(s.rate_limit_per_minute):
        _bucket = _TokenBucket(s.rate_limit_per_minute)
    return _bucket


class RateLimitMiddleware(BaseHTTPMiddleware):
    """Per-IP token-bucket rate limit on API endpoints. Static assets and
    public allowlist paths bypass the limiter so a hostile client can't
    DoS the dashboard's HTML by exhausting the bucket on `/`."""

    async def dispatch(self, request: Request, call_next):
        s = get_settings()
        if s.rate_limit_per_minute <= 0:
            return await call_next(request)
        if _is_public(request.url.path):
            return await call_next(request)
        # Source IP — prefer X-Forwarded-For when behind a trusted proxy,
        # else the direct client. We accept the leftmost X-Forwarded-For
        # value as the originating client; deployers are expected to
        # strip/control this header at the proxy boundary.
        xff = request.headers.get("x-forwarded-for", "")
        ip = (xff.split(",")[0].strip() if xff else None) or (
            request.client.host if request.client else "unknown"
        )
        allowed, retry = _get_bucket().consume(ip)
        if not allowed:
            return JSONResponse(
                status_code=429,
                content={
                    "detail": f"Rate limit exceeded ({s.rate_limit_per_minute} req/min). "
                              f"Retry in {retry:.1f}s.",
                },
                headers={"Retry-After": str(max(1, int(retry + 0.5)))},
            )
        return await call_next(request)


def reset_rate_limiter() -> None:
    """Test hook — drop the in-memory bucket so consecutive tests don't
    leak token state into each other."""
    global _bucket
    _bucket = None
