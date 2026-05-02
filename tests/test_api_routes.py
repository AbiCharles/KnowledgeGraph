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


def test_versions_list_route_404_on_unknown_bundle(stub_db):
    r = _client().get("/use_cases/no-such-bundle/versions")
    assert r.status_code == 404


def test_versions_list_route_returns_empty_when_no_archives(stub_db):
    r = _client().get("/use_cases/kf-mfg-workorder/versions")
    assert r.status_code == 200
    body = r.json()
    assert body["slug"] == "kf-mfg-workorder"
    assert isinstance(body["versions"], list)


def test_diff_route_404_on_unknown_stamp(stub_db):
    r = _client().get("/use_cases/kf-mfg-workorder/versions/20260101T000000Z/diff")
    assert r.status_code == 404


def test_generate_data_preview_returns_ttl_and_summary(stub_db):
    r = _client().post("/use_cases/kf-mfg-workorder/generate-data?count=3")
    assert r.status_code == 200
    body = r.json()
    assert body["replaced"] is False
    assert body["summary"]["total_nodes"] > 0
    assert "@prefix" in body["ttl"]


def test_generate_data_404_on_unknown_bundle(stub_db):
    r = _client().post("/use_cases/no-such/generate-data")
    assert r.status_code == 404


def test_generate_data_count_validation(stub_db):
    r = _client().post("/use_cases/kf-mfg-workorder/generate-data?count=0")
    assert r.status_code == 400


def test_ontology_edit_route_400_on_bad_kind(stub_db):
    r = _client().post("/use_cases/kf-mfg-workorder/ontology/add", json={"kind": "garbage"})
    assert r.status_code == 400


def test_ontology_edit_route_404_on_unknown_bundle(stub_db):
    r = _client().post("/use_cases/no-such/ontology/add", json={"kind": "class", "name": "X"})
    assert r.status_code == 404


def test_agent_conversations_list_returns_empty_initially(stub_db):
    r = _client().get("/agents/conversations")
    assert r.status_code == 200
    body = r.json()
    assert "slug" in body and "conversations" in body
    assert isinstance(body["conversations"], list)


def test_agent_conversations_get_unknown_404(stub_db):
    r = _client().get("/agents/conversations/does-not-exist-cid")
    assert r.status_code == 404


def test_capabilities_route_returns_multi_db_flag(stub_db):
    r = _client().get("/capabilities")
    assert r.status_code == 200
    body = r.json()
    assert "multi_database" in body
    assert "active_database" in body
    assert isinstance(body["multi_database"], bool)


def test_graph_snapshot_unknown_slug_404(stub_db, monkeypatch):
    # supports_multi_db() returns False with the stub DB so we never try to
    # talk to Neo4j; the 404 comes from registry.load() not finding the slug.
    r = _client().get("/graph/snapshot?slug=does-not-exist")
    assert r.status_code == 404


def test_schema_summary_returns_labels_and_rels(stub_db):
    """Active bundle's schema summary feeds the Cypher editor's autocomplete.
    Doesn't assume which bundle is active — just asserts the shape and that
    each declared label has its property list keyed under it."""
    r = _client().get("/schema/summary")
    assert r.status_code == 200
    body = r.json()
    assert {"prefix", "labels", "relationship_types", "properties_by_label"} <= set(body)
    assert body["labels"], "active bundle should declare at least one OWL class"
    for label in body["labels"]:
        assert label in body["properties_by_label"], f"missing property list for {label}"


def test_graph_snapshot_returns_payload_shape(stub_db, monkeypatch):
    """Stub run_on_database to return one node and one edge so we can assert
    the route serialises them correctly (prefix-stripping, manifest echo)."""
    import db
    monkeypatch.setattr(db, "supports_multi_db", lambda: False)

    def _fake_run(_db, cypher, params=None):
        if "RETURN elementId(n) AS id" in cypher:
            return [{"id": "n1", "type": "kf-mfg__WorkOrder", "p": {"uri": "x", "kf-mfg__woStatus": "OPEN"}}]
        if "RETURN elementId(a) AS source" in cypher:
            return [{"source": "n1", "target": "n2", "rel": "kf-mfg__assignedToEquipment"}]
        return []

    monkeypatch.setattr("api.routes.graph.run_on_database", _fake_run)
    r = _client().get("/graph/snapshot?slug=kf-mfg-workorder")
    assert r.status_code == 200
    body = r.json()
    assert body["slug"] == "kf-mfg-workorder"
    assert body["nodes"][0]["type"] == "WorkOrder"
    assert "uri" not in body["nodes"][0]["p"]
    assert body["nodes"][0]["p"]["woStatus"] == "OPEN"
    assert body["edges"][0]["rel"] == "assignedToEquipment"
    assert body["manifest"]["prefix"] == "kf-mfg"


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
