"""Pipeline analyser — LLM-driven analysis of pipeline runs.

LLM is stubbed at the chat() seam (the analyser now goes through
pipeline.llm.chat for multi-provider routing) so these run offline.
The data-sample fetcher is exercised against db.run_query stubbed to
return controlled rows so the PII redaction logic can be verified.
"""
import json
from types import SimpleNamespace

import pytest


def _fake_response(payload, in_tokens=120, out_tokens=80, model="stub-model"):
    """Mimic the .content / .response_metadata / .model surface the analyser
    reads — same shape that pipeline.llm.LlmResponse exposes."""
    return SimpleNamespace(
        content=payload if isinstance(payload, str) else json.dumps(payload),
        response_metadata={
            "token_usage": {"prompt_tokens": in_tokens, "completion_tokens": out_tokens}
        },
        model=model,
        provider="stub",
    )


def _stub_llm(monkeypatch, payload):
    """Make every chat() call from the analyser return `payload`."""
    from pipeline.refiner import pipeline_analyser as pa
    monkeypatch.setattr(pa, "chat", lambda system, user, *, json_mode=True: _fake_response(payload))


# ── PII redaction ────────────────────────────────────────────────────────────

@pytest.mark.parametrize("name,is_pii", [
    ("email", True),
    ("emailAddress", True),
    ("workEmail", True),
    ("phone", True),
    ("phoneNumber", True),
    ("mobilePhone", True),
    ("ssn", True),
    ("password", True),
    ("apiKey", True),
    ("accessToken", True),
    ("firstName", True),
    ("lastName", True),
    ("fullName", True),
    ("address", True),
    ("birthDate", True),
    ("dob", True),
    ("creditCard", True),
    ("ccNumber", True),
    ("orderId", False),
    ("status", False),
    ("createdAt", False),
    ("amount", False),
    ("addressId", False),     # ID-shaped, not the literal address
    ("title", False),
])
def test_pii_pattern_classification(name, is_pii):
    from pipeline.refiner.pipeline_analyser import _is_pii_property
    assert _is_pii_property(name) is is_pii, f"{name!r} should be {'PII' if is_pii else 'safe'}"


# ── Sample data fetcher ─────────────────────────────────────────────────────

def test_fetch_data_sample_redacts_pii_values(tmp_use_cases_dir, monkeypatch):
    """When _fetch_data_sample reads property dicts, any PII property
    value must be replaced with '<redacted>' before the dict leaves
    the server. Verified by checking the returned dict, since that's
    what would otherwise reach the LLM prompt."""
    from pipeline.refiner.pipeline_analyser import _fetch_data_sample
    from pipeline.use_case import UseCase

    bundle = tmp_use_cases_dir / "pii-test"
    bundle.mkdir()
    (bundle / "manifest.yaml").write_text(
        "slug: pii-test\nname: PII Test\nprefix: pt\n"
        "namespace: http://example.org/pt#\nin_scope_classes: [Customer]\n"
    )
    (bundle / "ontology.ttl").write_text("@prefix pt: <http://example.org/pt#> .\n")
    (bundle / "data.ttl").write_text("# empty\n")
    uc = UseCase.from_dir(bundle)

    def fake_run_query(cypher, _params=None):
        # Simulate two nodes returned with mixed PII + safe properties.
        return [
            {"p": {"pt__email": "alice@x.com", "pt__orderCount": 7, "pt__phone": "555-1234"}},
            {"p": {"pt__email": "bob@x.com",   "pt__orderCount": 3, "pt__fullName": "Bob Builder"}},
        ]

    import db
    monkeypatch.setattr(db, "run_query", fake_run_query)

    sample = _fetch_data_sample(uc, max_per_class=10)
    assert "Customer" in sample
    rows = sample["Customer"]
    assert len(rows) == 2
    # PII redacted; safe properties untouched.
    assert rows[0]["email"] == "<redacted>"
    assert rows[0]["phone"] == "<redacted>"
    assert rows[0]["orderCount"] == 7
    assert rows[1]["email"] == "<redacted>"
    assert rows[1]["fullName"] == "<redacted>"
    assert rows[1]["orderCount"] == 3


def test_fetch_data_sample_swallows_class_errors(tmp_use_cases_dir, monkeypatch):
    """A Cypher error for one class shouldn't abort the entire sample
    fetch — just that class is skipped."""
    from pipeline.refiner.pipeline_analyser import _fetch_data_sample
    from pipeline.use_case import UseCase

    bundle = tmp_use_cases_dir / "errors"
    bundle.mkdir()
    (bundle / "manifest.yaml").write_text(
        "slug: errors\nname: Errors\nprefix: e\n"
        "namespace: http://example.org/e#\nin_scope_classes: [A, B]\n"
    )
    (bundle / "ontology.ttl").write_text("@prefix e: <http://example.org/e#> .\n")
    (bundle / "data.ttl").write_text("# empty\n")
    uc = UseCase.from_dir(bundle)

    calls = {"n": 0}
    def fake_run_query(cypher, _p=None):
        calls["n"] += 1
        # First call (class A) → error. Second call (class B) → succeeds.
        if calls["n"] == 1:
            raise RuntimeError("Cypher exploded")
        return [{"p": {"e__id": 1}}]

    import db
    monkeypatch.setattr(db, "run_query", fake_run_query)

    sample = _fetch_data_sample(uc, max_per_class=10)
    assert "A" not in sample      # skipped due to error
    assert "B" in sample           # still returned


# ── analyse() main entry point ──────────────────────────────────────────────

def _seed_uc(tmp_use_cases_dir):
    """Tiny bundle good enough to call analyse() end-to-end."""
    from pipeline.use_case import UseCase
    bundle = tmp_use_cases_dir / "analyse-test"
    bundle.mkdir()
    (bundle / "manifest.yaml").write_text(
        "slug: analyse-test\nname: Analyse Test\nprefix: at\n"
        "namespace: http://example.org/at#\nin_scope_classes: [Order]\n"
    )
    (bundle / "ontology.ttl").write_text("@prefix at: <http://example.org/at#> .\n")
    (bundle / "data.ttl").write_text("# empty\n")
    return UseCase.from_dir(bundle)


def test_analyse_returns_findings_in_canonical_shape(tmp_use_cases_dir, monkeypatch):
    """End-to-end: stubbed LLM returns 2 findings; analyse() normalises
    them, prefixes IDs with 'llm-pipe-', tags source, and returns
    counts/by_category."""
    _stub_llm(monkeypatch, {
        "findings": [
            {"id": "fix-orderid-label", "severity": "warn", "category": "labels",
             "title": "Add label to orderId", "description": "It looks bare.",
             "fix": {"kind": "add_label", "target": "property:orderId", "value": "Order ID"}},
            {"id": "review-pull-sql", "severity": "error", "category": "ingestion",
             "title": "Pull SQL returned 0 rows", "description": "Check the WHERE clause.",
             "fix": {"kind": "noop", "target": "stage:4", "preview": "review SQL"}},
        ]
    })
    uc = _seed_uc(tmp_use_cases_dir)
    from pipeline.refiner.pipeline_analyser import analyse
    res = analyse(uc, [
        {"stage": 4, "name": "Live Data Ingestion", "status": "fail", "logs": ["FAIL  Adapter X pulled 0 rows"], "error": "0 rows"},
    ])
    assert res["total"] == 2
    assert res["counts"] == {"error": 1, "warn": 1, "info": 0}
    ids = [f["id"] for f in res["findings"]]
    assert all(i.startswith("llm-pipe-") for i in ids)
    assert all(f["source"] == "llm-pipeline" for f in res["findings"])


def test_analyse_failed_only_returns_empty_when_nothing_failed(tmp_use_cases_dir, monkeypatch):
    """When failed_only=True (auto-on-FAIL path) and every stage
    PASSed, skip the LLM call entirely — saves credits."""
    _stub_llm(monkeypatch, {"findings": []})
    uc = _seed_uc(tmp_use_cases_dir)
    from pipeline.refiner.pipeline_analyser import analyse
    res = analyse(uc, [
        {"stage": 0, "name": "Preflight", "status": "pass", "logs": []},
    ], failed_only=True)
    assert res["total"] == 0
    assert res["findings"] == []


def test_analyse_full_run_includes_pass_stages(tmp_use_cases_dir, monkeypatch):
    """When failed_only=False (manual button path), pass stages stay in
    the prompt so the LLM can suggest improvements even on green runs."""
    captured = {}
    def _stub_chat(system, user, *, json_mode=True):
        captured["body"] = user
        return _fake_response({"findings": []})

    from pipeline.refiner import pipeline_analyser as pa
    monkeypatch.setattr(pa, "chat", _stub_chat)

    uc = _seed_uc(tmp_use_cases_dir)
    pa.analyse(uc, [
        {"stage": 0, "name": "Preflight", "status": "pass", "logs": ["PASS Neo4j connected"]},
        {"stage": 1, "name": "Wipe & Init", "status": "pass", "logs": ["PASS DB wiped"]},
    ], failed_only=False)
    # Both stages should appear in the JSON the prompt was built from.
    assert "Preflight" in captured["body"]
    assert "Wipe & Init" in captured["body"]


def test_analyse_handles_invalid_json_response(tmp_use_cases_dir, monkeypatch):
    """LLM occasionally returns non-JSON despite response_format. analyse()
    should degrade with error field, not crash."""
    _stub_llm(monkeypatch, "this is not JSON")
    uc = _seed_uc(tmp_use_cases_dir)
    from pipeline.refiner.pipeline_analyser import analyse
    res = analyse(uc, [{"stage": 1, "name": "X", "status": "fail", "logs": []}])
    assert res["total"] == 0
    assert "error" in res
    assert "JSON" in res["error"]


def test_analyse_respects_daily_cost_cap(tmp_use_cases_dir, monkeypatch):
    """Cap-hit path should return cap_hit=True without invoking the LLM."""
    from pipeline.refiner import pipeline_analyser as pa
    from fastapi import HTTPException
    def boom():
        raise HTTPException(status_code=429, detail="Daily cap reached")
    monkeypatch.setattr(pa, "assert_within_daily_cap", boom)
    # Don't even bother stubbing LLM — this should never reach it.
    uc = _seed_uc(tmp_use_cases_dir)
    res = pa.analyse(uc, [{"stage": 1, "name": "X", "status": "fail", "logs": []}])
    assert res.get("cap_hit") is True
    assert res["total"] == 0


# ── Route smoke ─────────────────────────────────────────────────────────────

def test_analyse_pipeline_route_404_unknown_bundle(stub_db):
    from fastapi.testclient import TestClient
    from api.main import app
    r = TestClient(app).post("/refine/no-such/analyse-pipeline", json={"stage_logs": []})
    assert r.status_code == 404


def test_analyse_pipeline_route_returns_findings(stub_db, monkeypatch):
    """End-to-end through the FastAPI route, stubbed LLM."""
    _stub_llm(monkeypatch, {"findings": [
        {"id": "x", "severity": "info", "category": "structure",
         "title": "ok", "description": "...", "fix": {"kind": "noop", "target": ""}}
    ]})
    from fastapi.testclient import TestClient
    from api.main import app
    r = TestClient(app).post(
        "/refine/kf-mfg-workorder/analyse-pipeline",
        json={"stage_logs": [{"stage": 1, "name": "X", "status": "fail", "logs": []}]},
    )
    assert r.status_code == 200
    body = r.json()
    assert body["total"] == 1
    assert body["findings"][0]["source"] == "llm-pipeline"
