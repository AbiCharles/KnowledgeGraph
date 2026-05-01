"""Stage 6 dispatcher: critical-vs-warning, multi-row cypher, strict bool."""
from pathlib import Path

import pytest

from pipeline.use_case import CheckSpec, UseCase, Manifest
from pipeline import stage6_validate


def _ctx(_rows_for=None, manifest_kwargs=None):
    """Build a minimal ctx; validate() never touches the bundle paths."""
    manifest_kwargs = {**dict(slug="t", name="T", prefix="t", namespace="http://x/", in_scope_classes=["Foo"]),
                      **(manifest_kwargs or {})}
    m = Manifest(**manifest_kwargs)
    uc = UseCase(manifest=m, bundle_dir=Path("/nonexistent"))
    return {"use_case": uc}


def test_count_at_least_pass(monkeypatch):
    monkeypatch.setattr(stage6_validate, "run_query", lambda q, p=None: [{"n": 5}])
    ctx = _ctx({})
    ctx["use_case"].manifest.stage6_checks = [
        CheckSpec(id="X", kind="count_at_least", label="Foo", threshold=3, severity="critical")
    ]
    logs = stage6_validate.validate(ctx)
    assert any("PASS" in l for l in logs)


def test_count_at_least_critical_fail_raises(monkeypatch):
    monkeypatch.setattr(stage6_validate, "run_query", lambda q, p=None: [{"n": 1}])
    ctx = _ctx({})
    ctx["use_case"].manifest.stage6_checks = [
        CheckSpec(id="X", kind="count_at_least", label="Foo", threshold=3, severity="critical")
    ]
    with pytest.raises(RuntimeError) as ei:
        stage6_validate.validate(ctx)
    assert "X" in str(ei.value)


def test_count_at_least_warning_does_not_raise(monkeypatch):
    monkeypatch.setattr(stage6_validate, "run_query", lambda q, p=None: [{"n": 0}])
    ctx = _ctx({})
    ctx["use_case"].manifest.stage6_checks = [
        CheckSpec(id="X", kind="count_at_least", label="Foo", threshold=1, severity="warning")
    ]
    logs = stage6_validate.validate(ctx)
    assert any("WARN" in l and "X" in l for l in logs)


def test_cypher_check_strict_bool_rejects_string(monkeypatch):
    """A check returning passed='false' must NOT silently pass."""
    monkeypatch.setattr(stage6_validate, "run_query", lambda q, p=None: [{"passed": "false"}])
    ctx = _ctx({})
    ctx["use_case"].manifest.stage6_checks = [
        CheckSpec(id="STR", kind="cypher",
                  cypher="MATCH (n) RETURN 'false' AS passed",
                  severity="warning")
    ]
    logs = stage6_validate.validate(ctx)
    # Should be a failure (logged as WARN, not raised)
    assert any("WARN" in l and "STR" in l for l in logs)


def test_cypher_check_multi_row_requires_all_pass(monkeypatch):
    """Multi-row cypher checks must require every row to be true."""
    monkeypatch.setattr(stage6_validate, "run_query",
                        lambda q, p=None: [{"passed": True}, {"passed": False}, {"passed": True}])
    ctx = _ctx({})
    ctx["use_case"].manifest.stage6_checks = [
        CheckSpec(id="MULTI", kind="cypher", cypher="MATCH (n) RETURN n.ok AS passed",
                  severity="warning")
    ]
    logs = stage6_validate.validate(ctx)
    assert any("WARN" in l and "MULTI" in l for l in logs)


def test_cypher_check_missing_passed_column(monkeypatch):
    monkeypatch.setattr(stage6_validate, "run_query", lambda q, p=None: [{"foo": 1}])
    ctx = _ctx({})
    ctx["use_case"].manifest.stage6_checks = [
        CheckSpec(id="NOPE", kind="cypher", cypher="MATCH (n) RETURN n.foo AS foo",
                  severity="warning")
    ]
    logs = stage6_validate.validate(ctx)
    assert any("WARN" in l and "passed" in l for l in logs)


def test_generic_checks_when_no_manifest_checks(monkeypatch):
    monkeypatch.setattr(stage6_validate, "run_query", lambda q, p=None: [{"n": 7}])
    ctx = _ctx({})
    ctx["use_case"].manifest.stage6_checks = []
    logs = stage6_validate.validate(ctx)
    # One auto check per declared in_scope class.
    assert any("VC-AUTO-01" in l for l in logs)


def test_no_checks_no_classes_warns_not_silent(monkeypatch):
    monkeypatch.setattr(stage6_validate, "run_query", lambda q, p=None: [])
    ctx = _ctx({}, manifest_kwargs={"in_scope_classes": []})
    ctx["use_case"].manifest.stage6_checks = []
    logs = stage6_validate.validate(ctx)
    assert any("WARN" in l for l in logs)
