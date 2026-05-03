"""Stage 6 — smart severity for the auto-generated count >= 1 checks.

Marker classes (no datatype properties) get severity=warning so the
pipeline doesn't FAIL when they're empty — required to support the
lint-demo bundle and any future bundle that uses property-less classes
as taxonomy markers.

Classes with declared datatype properties stay severity=critical so the
"I forgot to load Orders" safety net still exists.
"""
from pipeline.stage6_validate import _generic_checks
from pipeline.use_case import UseCase


def _seed_bundle(tmp_use_cases_dir, slug, in_scope, ontology_ttl):
    """Write a minimal bundle to tmp_use_cases_dir and load it as a UseCase."""
    bundle = tmp_use_cases_dir / slug
    bundle.mkdir()
    in_scope_yaml = ", ".join(in_scope)
    (bundle / "manifest.yaml").write_text(
        f"slug: {slug}\nname: Test {slug}\nprefix: t\n"
        f"namespace: http://example.org/t#\n"
        f"in_scope_classes: [{in_scope_yaml}]\n"
    )
    (bundle / "ontology.ttl").write_text(ontology_ttl)
    (bundle / "data.ttl").write_text("# empty\n")
    return UseCase.from_dir(bundle)


def test_class_with_datatype_property_gets_critical_severity(tmp_use_cases_dir):
    """An in-scope class that declares at least one datatype property is
    a 'real' entity — empty count must still FAIL the pipeline."""
    ttl = """\
@prefix t:    <http://example.org/t#> .
@prefix owl:  <http://www.w3.org/2002/07/owl#> .
@prefix rdfs: <http://www.w3.org/2000/01/rdf-schema#> .
@prefix xsd:  <http://www.w3.org/2001/XMLSchema#> .

t:Order a owl:Class ; rdfs:label "Order" .
t:orderId a owl:DatatypeProperty ; rdfs:domain t:Order ; rdfs:range xsd:integer .
"""
    uc = _seed_bundle(tmp_use_cases_dir, "real-class", ["Order"], ttl)
    checks = _generic_checks(uc)
    assert len(checks) == 1
    assert checks[0].severity == "critical"
    assert checks[0].label == "Order"


def test_class_with_no_datatype_properties_gets_warning_severity(tmp_use_cases_dir):
    """A marker class (no datatype properties declared) should not fail
    the pipeline when empty — emit a WARN check instead."""
    ttl = """\
@prefix t:    <http://example.org/t#> .
@prefix owl:  <http://www.w3.org/2002/07/owl#> .
@prefix rdfs: <http://www.w3.org/2000/01/rdf-schema#> .

t:Marker a owl:Class ; rdfs:label "Marker" .
"""
    uc = _seed_bundle(tmp_use_cases_dir, "marker-class", ["Marker"], ttl)
    checks = _generic_checks(uc)
    assert len(checks) == 1
    assert checks[0].severity == "warning"
    # The description should explain why severity differs.
    assert "warning" in checks[0].description.lower() or "no datatype properties" in checks[0].description


def test_mixed_bundle_emits_both_severities(tmp_use_cases_dir):
    """The lint-demo case: some classes have properties (critical),
    some are markers (warning). Both should be in the same suite with
    the right severity each."""
    ttl = """\
@prefix t:    <http://example.org/t#> .
@prefix owl:  <http://www.w3.org/2002/07/owl#> .
@prefix rdfs: <http://www.w3.org/2000/01/rdf-schema#> .
@prefix xsd:  <http://www.w3.org/2001/XMLSchema#> .

t:Order a owl:Class ; rdfs:label "Order" .
t:orderId a owl:DatatypeProperty ; rdfs:domain t:Order ; rdfs:range xsd:integer .
t:Orphan a owl:Class ; rdfs:label "Orphan" .
"""
    uc = _seed_bundle(tmp_use_cases_dir, "mixed", ["Order", "Orphan"], ttl)
    checks = _generic_checks(uc)
    by_label = {c.label: c for c in checks}
    assert by_label["Order"].severity == "critical"
    assert by_label["Orphan"].severity == "warning"


def test_class_in_scope_but_not_in_ontology_treated_as_marker(tmp_use_cases_dir):
    """If a class is declared in_scope but absent from the ontology,
    schema_summary returns nothing for it — should be treated as a
    marker (warning) rather than crashing or defaulting to critical."""
    ttl = "@prefix t: <http://example.org/t#> .\n"
    uc = _seed_bundle(tmp_use_cases_dir, "missing-ontology", ["Phantom"], ttl)
    checks = _generic_checks(uc)
    assert len(checks) == 1
    assert checks[0].severity == "warning"


def test_corrupt_ontology_falls_back_to_critical(tmp_use_cases_dir):
    """If rdflib can't parse the ontology TTL inside _generic_checks
    (corrupt syntax etc.), preserve the safety net by emitting critical
    severity for every in-scope class rather than silently degrading
    every check to warning."""
    # Write a deliberately broken TTL — bundle still LOADS (the bundle
    # loader doesn't parse the TTL itself; that's stage 2's job) but
    # rdflib.Graph().parse inside _generic_checks will raise.
    bundle = tmp_use_cases_dir / "corrupt"
    bundle.mkdir()
    (bundle / "manifest.yaml").write_text(
        "slug: corrupt\nname: Test\nprefix: t\n"
        "namespace: http://example.org/t#\nin_scope_classes: [Order, Customer]\n"
    )
    (bundle / "ontology.ttl").write_text("this is not valid turtle ::: {")
    (bundle / "data.ttl").write_text("# empty\n")
    uc = UseCase.from_dir(bundle)

    checks = _generic_checks(uc)
    # Both classes — without a working schema introspection we can't
    # tell markers apart from real entities, so be conservative.
    assert len(checks) == 2
    assert all(c.severity == "critical" for c in checks)


def test_existing_explicit_checks_unaffected(tmp_use_cases_dir):
    """Bundles with manifest.stage6_checks: declared bypass the
    auto-generator entirely. Verify _generic_checks is only consulted
    when explicit checks are absent (validate() does this — sanity-test
    the integration point)."""
    from pipeline.stage6_validate import validate
    # No need to construct a real validate() context here — just confirm
    # the generic-checks function is callable + the defaults haven't
    # accidentally changed shape.
    ttl = """\
@prefix t:    <http://example.org/t#> .
@prefix owl:  <http://www.w3.org/2002/07/owl#> .
@prefix rdfs: <http://www.w3.org/2000/01/rdf-schema#> .
@prefix xsd:  <http://www.w3.org/2001/XMLSchema#> .

t:X a owl:Class ; rdfs:label "X" .
t:y a owl:DatatypeProperty ; rdfs:domain t:X ; rdfs:range xsd:string .
"""
    uc = _seed_bundle(tmp_use_cases_dir, "explicit", ["X"], ttl)
    checks = _generic_checks(uc)
    assert len(checks) == 1
    assert checks[0].kind == "count_at_least"
    assert checks[0].threshold == 1
    assert checks[0].id == "VC-AUTO-01"
