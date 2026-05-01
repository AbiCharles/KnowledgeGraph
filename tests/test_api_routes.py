"""FastAPI route smoke tests with the DB stubbed out.

Confirms wiring, schema serialisation, CORS lockdown, and that the cypher
safety filter is actually plumbed at the /query endpoint.
"""
from fastapi.testclient import TestClient


def _client():
    from api.main import app
    return TestClient(app)


def test_health_ok(stub_db):
    r = _client().get("/health")
    assert r.status_code == 200
    assert r.json() == {"status": "ok"}


def test_query_blocks_writes(stub_db):
    r = _client().post("/query", json={"cypher": "MATCH (n) DETACH DELETE n"})
    assert r.status_code == 400
    assert "DELETE" in r.json()["detail"] or "DETACH" in r.json()["detail"]


def test_query_passes_safe_query(stub_db):
    stub_db["RETURN n"] = [{"n": 1}]
    r = _client().post("/query", json={"cypher": "MATCH (n) RETURN n LIMIT 1"})
    assert r.status_code == 200
    body = r.json()
    assert body["row_count"] == 1
    assert body["columns"] == ["n"]


def test_use_cases_list_returns_summary_shape(stub_db):
    r = _client().get("/use_cases")
    assert r.status_code == 200
    body = r.json()
    assert "active" in body and "bundles" in body
    # Both shipped bundles should be visible
    slugs = {b["slug"] for b in body["bundles"]}
    assert "kf-mfg-workorder" in slugs


def test_set_active_bad_body_422(stub_db):
    r = _client().post("/use_cases/active", json={})  # missing slug
    assert r.status_code == 422  # Pydantic-validated


def test_set_active_unknown_slug_404(stub_db):
    r = _client().post("/use_cases/active", json={"slug": "does-not-exist"})
    assert r.status_code == 404


def test_get_use_case_by_slug_returns_manifest(stub_db):
    r = _client().get("/use_cases/kf-mfg-workorder")
    assert r.status_code == 200
    body = r.json()
    assert body["slug"] == "kf-mfg-workorder"
    assert body["prefix"] == "kf-mfg"


def test_get_unknown_use_case_404(stub_db):
    r = _client().get("/use_cases/nope")
    assert r.status_code == 404


def test_cors_safe_invariant():
    """If origins include '*', credentials must be disabled. Either way the
    app must never combine wildcard origins with credentials enabled."""
    from api.main import app
    cors = next(
        (m for m in app.user_middleware if m.cls.__name__ == "CORSMiddleware"),
        None,
    )
    assert cors is not None
    opts = cors.kwargs
    if "*" in opts.get("allow_origins", []):
        assert opts.get("allow_credentials") is False, \
            "CORS allow_credentials must be False when allow_origins includes '*'"


def test_pipeline_run_lock_returns_409_when_busy(stub_db, monkeypatch):
    """While a pipeline run is in flight, a second POST gets 409."""
    from api import locks
    # Pre-acquire the lock to simulate an in-flight run.
    import asyncio
    loop = asyncio.new_event_loop()
    try:
        loop.run_until_complete(locks.pipeline_lock.acquire())
        r = _client().post("/pipeline/run", json={})
        assert r.status_code == 409
    finally:
        if locks.pipeline_lock.locked():
            locks.pipeline_lock.release()
        loop.close()
