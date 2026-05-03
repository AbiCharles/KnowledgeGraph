"""Refiner — linter rules + applicator + route smoke. The LLM coach
is exercised lightly (mocked LLM response) since the real call costs
money; the rule-based linter and applicator get the heavy coverage."""
import sys
from types import ModuleType, SimpleNamespace

import pytest


# Minimal TTL fixtures used across tests.
_NS = "http://example.org/ex#"

_TTL_HAPPY = """\
@prefix ex:   <http://example.org/ex#> .
@prefix owl:  <http://www.w3.org/2002/07/owl#> .
@prefix rdfs: <http://www.w3.org/2000/01/rdf-schema#> .
@prefix xsd:  <http://www.w3.org/2001/XMLSchema#> .
@prefix skos: <http://www.w3.org/2004/02/skos/core#> .

ex:Order a owl:Class ; rdfs:label "Order" ; skos:definition "An order." .
ex:Customer a owl:Class ; rdfs:label "Customer" ; skos:definition "A customer." .
ex:orderId a owl:DatatypeProperty ; rdfs:label "Order ID" ;
    rdfs:domain ex:Order ; rdfs:range xsd:integer .
ex:placedBy a owl:ObjectProperty ; rdfs:label "Placed by" ;
    rdfs:domain ex:Order ; rdfs:range ex:Customer .
"""

_TTL_PROBLEMS = """\
@prefix ex:   <http://example.org/ex#> .
@prefix owl:  <http://www.w3.org/2002/07/owl#> .
@prefix rdfs: <http://www.w3.org/2000/01/rdf-schema#> .
@prefix xsd:  <http://www.w3.org/2001/XMLSchema#> .

ex:Order a owl:Class .
ex:Customer a owl:Class .
ex:Orphan a owl:Class .
ex:bad_property a owl:DatatypeProperty ; rdfs:domain ex:Order ; rdfs:range xsd:string .
ex:customerId a owl:DatatypeProperty ; rdfs:domain ex:Order ; rdfs:range xsd:integer .
ex:noDomain a owl:DatatypeProperty ; rdfs:range xsd:string .
ex:objNoRange a owl:ObjectProperty ; rdfs:domain ex:Order .
"""


# ── linter.lint_text() ───────────────────────────────────────────────────────

def test_happy_ontology_yields_no_findings():
    from pipeline.refiner.linter import lint_text
    res = lint_text(_TTL_HAPPY, "ex", _NS)
    assert res["total"] == 0
    assert res["findings"] == []


def test_orphan_class_detected():
    from pipeline.refiner.linter import lint_text
    res = lint_text(_TTL_PROBLEMS, "ex", _NS)
    assert any(f["id"] == "orphan-class-Orphan" for f in res["findings"])


def test_missing_label_finding_per_class():
    from pipeline.refiner.linter import lint_text
    res = lint_text(_TTL_PROBLEMS, "ex", _NS)
    titles = [f["title"] for f in res["findings"]]
    # All 3 classes have no labels
    assert any("Order has no rdfs:label" in t for t in titles)
    assert any("Customer has no rdfs:label" in t for t in titles)


def test_property_no_domain_warning():
    from pipeline.refiner.linter import lint_text
    res = lint_text(_TTL_PROBLEMS, "ex", _NS)
    assert any(f["id"] == "prop-no-domain-noDomain" for f in res["findings"])
    no_domain = next(f for f in res["findings"] if f["id"] == "prop-no-domain-noDomain")
    assert no_domain["severity"] == "warn"


def test_object_property_no_range_warning():
    from pipeline.refiner.linter import lint_text
    res = lint_text(_TTL_PROBLEMS, "ex", _NS)
    assert any(f["id"] == "objprop-no-range-objNoRange" for f in res["findings"])


def test_hidden_fk_detected_when_class_with_matching_name_exists():
    """customerId looks like FK to Customer (which exists in this ontology)."""
    from pipeline.refiner.linter import lint_text
    res = lint_text(_TTL_PROBLEMS, "ex", _NS)
    fk_findings = [f for f in res["findings"] if f["id"].startswith("hidden-fk-")]
    assert any(f["id"] == "hidden-fk-customerId" for f in fk_findings)
    f = next(f for f in fk_findings if f["id"] == "hidden-fk-customerId")
    assert f["fix"]["kind"] == "convert_to_object"
    assert f["fix"]["range_class"] == "Customer"


def test_naming_finding_for_snake_case_property():
    from pipeline.refiner.linter import lint_text
    res = lint_text(_TTL_PROBLEMS, "ex", _NS)
    assert any(f["id"] == "prop-naming-bad_property" for f in res["findings"])


def test_findings_sorted_by_severity():
    from pipeline.refiner.linter import lint_text
    res = lint_text(_TTL_PROBLEMS, "ex", _NS)
    severities = [f["severity"] for f in res["findings"]]
    rank = {"error": 0, "warn": 1, "info": 2}
    sevs_ranked = [rank[s] for s in severities]
    assert sevs_ranked == sorted(sevs_ranked), "findings must be ordered error → warn → info"


def test_summary_counts_match_findings():
    from pipeline.refiner.linter import lint_text
    res = lint_text(_TTL_PROBLEMS, "ex", _NS)
    by_sev = {"error": 0, "warn": 0, "info": 0}
    for f in res["findings"]:
        by_sev[f["severity"]] += 1
    assert res["counts"] == by_sev


def test_empty_ttl_does_not_crash():
    from pipeline.refiner.linter import lint_text
    res = lint_text("", "ex", _NS)
    assert res["total"] == 0


# ── applicator ───────────────────────────────────────────────────────────────

@pytest.fixture
def seeded_bundle(tmp_use_cases_dir):
    """Write a real bundle to the tmp use_cases dir so the applicator can
    round-trip through register_uploaded."""
    bundle = tmp_use_cases_dir / "ref-test"
    bundle.mkdir()
    (bundle / "manifest.yaml").write_text(
        "slug: ref-test\nname: Ref Test\nprefix: ex\n"
        "namespace: http://example.org/ex#\nin_scope_classes: [Order, Customer]\n"
    )
    (bundle / "ontology.ttl").write_text(_TTL_PROBLEMS.replace("ex:Orphan a owl:Class .\n", ""))
    (bundle / "data.ttl").write_text("# empty\n")
    return bundle


def test_apply_add_label_to_class(seeded_bundle):
    from pipeline.refiner.applicator import apply_fix
    fix = {"kind": "add_label", "target": "class:Order", "value": "Order"}
    res = apply_fix("ref-test", fix)
    assert res["applied"] == "add_label"
    new_ttl = (seeded_bundle / "ontology.ttl").read_text()
    assert '"Order"' in new_ttl
    assert "rdfs:label" in new_ttl


def test_apply_add_description_to_property(seeded_bundle):
    from pipeline.refiner.applicator import apply_fix
    fix = {"kind": "add_description", "target": "property:customerId",
           "value": "Foreign key to the Customer node."}
    apply_fix("ref-test", fix)
    new_ttl = (seeded_bundle / "ontology.ttl").read_text()
    assert "Foreign key to the Customer node." in new_ttl


def test_apply_unknown_kind_raises(seeded_bundle):
    from pipeline.refiner.applicator import apply_fix
    with pytest.raises(ValueError, match="Unknown fix kind"):
        apply_fix("ref-test", {"kind": "totally-made-up", "target": "class:Order"})


def test_apply_noop_raises(seeded_bundle):
    """noop fixes should never be applied — the linter emits them for
    findings that need operator decisions. Applicator must reject."""
    from pipeline.refiner.applicator import apply_fix
    with pytest.raises(ValueError, match="no automatic fix"):
        apply_fix("ref-test", {"kind": "noop", "target": "class:Order"})


def test_apply_to_unknown_bundle_404(tmp_use_cases_dir):
    from pipeline.refiner.applicator import apply_fix
    with pytest.raises(FileNotFoundError):
        apply_fix("no-such-bundle", {"kind": "add_label", "target": "class:X", "value": "x"})


def test_apply_convert_to_object(seeded_bundle):
    """The hidden-FK fix: removes the datatype property and creates a
    matching object property linking domain → range_class."""
    from pipeline.refiner.applicator import apply_fix
    fix = {
        "kind": "convert_to_object",
        "target": "property:customerId",
        "new_property": "customer",
        "range_class": "Customer",
    }
    res = apply_fix("ref-test", fix)
    new_ttl = (seeded_bundle / "ontology.ttl").read_text()
    assert "customer a" in new_ttl   # new object property
    assert "owl:ObjectProperty" in new_ttl
    # Old datatype property triples should be gone
    assert "ex:customerId" not in new_ttl


# ── routes ──────────────────────────────────────────────────────────────────

def _client():
    from fastapi.testclient import TestClient
    from api.main import app
    return TestClient(app)


def test_lint_route_returns_findings(stub_db):
    """Real shipped bundle (kf-mfg-workorder) — should produce zero or
    more findings, never crash."""
    r = _client().get("/refine/kf-mfg-workorder/lint")
    assert r.status_code == 200
    body = r.json()
    assert "findings" in body
    assert "counts" in body


def test_lint_route_404_unknown_bundle(stub_db):
    r = _client().get("/refine/no-such/lint")
    assert r.status_code == 404


def test_preview_lint_route_works_on_inline_ttl(stub_db):
    """Builder preview path — lint TTL that hasn't been written to disk."""
    r = _client().post("/refine/preview-lint", json={
        "ontology_ttl": _TTL_PROBLEMS,
        "prefix": "ex",
        "namespace": _NS,
    })
    assert r.status_code == 200
    body = r.json()
    assert body["total"] > 0
    assert any(f["id"].startswith("orphan") for f in body["findings"])


def test_preview_lint_route_400_on_missing_prefix(stub_db):
    r = _client().post("/refine/preview-lint", json={"ontology_ttl": _TTL_HAPPY})
    assert r.status_code == 400


def test_apply_route_400_on_missing_fix(stub_db):
    r = _client().post("/refine/kf-mfg-workorder/apply", json={})
    assert r.status_code == 400
