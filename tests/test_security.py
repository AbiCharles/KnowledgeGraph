"""Auth + rate-limit middleware. Both default-off so existing tests pass
without modification — these tests flip the relevant Settings field on per
test via monkeypatch + cache clear."""
from fastapi.testclient import TestClient


def _client_with(monkeypatch, **overrides):
    """Spin up a fresh TestClient with overridden Settings. Settings has an
    @lru_cache, so we monkeypatch env vars + clear the cache."""
    from config import get_settings
    for key, val in overrides.items():
        monkeypatch.setenv(key.upper(), str(val))
    get_settings.cache_clear()
    # Reset rate-limit bucket so tests don't share state.
    from api.security import reset_rate_limiter
    reset_rate_limiter()
    from api.main import app
    return TestClient(app)


def test_auth_disabled_when_api_key_unset(stub_db):
    from fastapi.testclient import TestClient
    from api.main import app
    r = TestClient(app).get("/use_cases")
    assert r.status_code == 200  # no key configured → no auth check


def test_auth_blocks_missing_header(stub_db, monkeypatch):
    c = _client_with(monkeypatch, api_key="secret-test-key")
    try:
        r = c.get("/use_cases")
        assert r.status_code == 401
        assert "API key" in r.json()["detail"]
    finally:
        monkeypatch.delenv("API_KEY", raising=False)
        from config import get_settings
        get_settings.cache_clear()


def test_auth_accepts_correct_header(stub_db, monkeypatch):
    c = _client_with(monkeypatch, api_key="secret-test-key")
    try:
        r = c.get("/use_cases", headers={"X-API-Key": "secret-test-key"})
        assert r.status_code == 200
    finally:
        monkeypatch.delenv("API_KEY", raising=False)
        from config import get_settings
        get_settings.cache_clear()


def test_auth_rejects_wrong_header(stub_db, monkeypatch):
    c = _client_with(monkeypatch, api_key="secret-test-key")
    try:
        r = c.get("/use_cases", headers={"X-API-Key": "totally-wrong"})
        assert r.status_code == 401
    finally:
        monkeypatch.delenv("API_KEY", raising=False)
        from config import get_settings
        get_settings.cache_clear()


def test_auth_health_and_capabilities_always_public(stub_db, monkeypatch):
    """Public allowlist must let the dashboard boot even before login."""
    c = _client_with(monkeypatch, api_key="secret-test-key")
    try:
        assert c.get("/health").status_code == 200
        assert c.get("/capabilities").status_code == 200
        # OpenAPI docs also reachable.
        assert c.get("/openapi.json").status_code == 200
    finally:
        monkeypatch.delenv("API_KEY", raising=False)
        from config import get_settings
        get_settings.cache_clear()


def test_rate_limit_disabled_when_zero(stub_db, monkeypatch):
    c = _client_with(monkeypatch, rate_limit_per_minute="0")
    try:
        for _ in range(20):
            assert c.get("/use_cases").status_code == 200
    finally:
        monkeypatch.delenv("RATE_LIMIT_PER_MINUTE", raising=False)
        from config import get_settings
        get_settings.cache_clear()


def test_rate_limit_blocks_after_burst(stub_db, monkeypatch):
    """Bucket caps at 3; the 4th request inside the burst window 429s."""
    c = _client_with(monkeypatch, rate_limit_per_minute="3")
    try:
        # First 3 should pass (initial bucket = 3 tokens).
        for _ in range(3):
            assert c.get("/use_cases").status_code == 200
        # 4th should 429.
        r = c.get("/use_cases")
        assert r.status_code == 429
        assert "Rate limit" in r.json()["detail"]
        assert "Retry-After" in r.headers
    finally:
        monkeypatch.delenv("RATE_LIMIT_PER_MINUTE", raising=False)
        from config import get_settings
        get_settings.cache_clear()


def test_rate_limit_does_not_apply_to_health(stub_db, monkeypatch):
    """Public paths bypass the limiter so /health stays a reliable probe."""
    c = _client_with(monkeypatch, rate_limit_per_minute="2")
    try:
        for _ in range(10):
            assert c.get("/health").status_code == 200
    finally:
        monkeypatch.delenv("RATE_LIMIT_PER_MINUTE", raising=False)
        from config import get_settings
        get_settings.cache_clear()
