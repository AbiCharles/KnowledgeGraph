"""Synthetic-data generator: parseable output, schema fidelity, determinism."""
from rdflib import Graph, RDF, URIRef

from pipeline.data_generator import generate_data


SAMPLE_ONTOLOGY = """\
@prefix ex:   <http://example.org/test#> .
@prefix owl:  <http://www.w3.org/2002/07/owl#> .
@prefix rdfs: <http://www.w3.org/2000/01/rdf-schema#> .
@prefix xsd:  <http://www.w3.org/2001/XMLSchema#> .

ex:Person a owl:Class ; rdfs:label "Person" .
ex:Company a owl:Class ; rdfs:label "Company" .

ex:fullName a owl:DatatypeProperty ;
    rdfs:domain ex:Person ; rdfs:range xsd:string .
ex:age a owl:DatatypeProperty ;
    rdfs:domain ex:Person ; rdfs:range xsd:integer .
ex:active a owl:DatatypeProperty ;
    rdfs:domain ex:Company ; rdfs:range xsd:boolean .

ex:worksAt a owl:ObjectProperty ;
    rdfs:domain ex:Person ; rdfs:range ex:Company ;
    owl:maxCardinality "1"^^xsd:nonNegativeInteger .
"""

NS = "http://example.org/test#"


def test_empty_ontology_returns_empty():
    ttl, summary = generate_data("@prefix x: <http://x#> .", NS, count=5)
    assert ttl == ""
    assert summary["total_nodes"] == 0
    assert summary["total_edges"] == 0


def test_generates_count_instances_per_class():
    ttl, summary = generate_data(SAMPLE_ONTOLOGY, NS, count=4)
    assert summary["total_nodes"] == 8  # 2 classes × 4
    by_class = {c["class"]: c["count"] for c in summary["classes"]}
    assert by_class == {"Person": 4, "Company": 4}


def test_output_is_valid_turtle_with_typed_instances():
    ttl, _ = generate_data(SAMPLE_ONTOLOGY, NS, count=3)
    g = Graph()
    g.parse(data=ttl, format="turtle")
    persons = list(g.subjects(RDF.type, URIRef(NS + "Person")))
    companies = list(g.subjects(RDF.type, URIRef(NS + "Company")))
    assert len(persons) == 3
    assert len(companies) == 3


def test_object_property_targets_only_declared_range():
    ttl, _ = generate_data(SAMPLE_ONTOLOGY, NS, count=5)
    g = Graph()
    g.parse(data=ttl, format="turtle")
    company_set = set(g.subjects(RDF.type, URIRef(NS + "Company")))
    for s, _, o in g.triples((None, URIRef(NS + "worksAt"), None)):
        assert o in company_set, f"worksAt edge points to non-Company {o}"


def test_functional_property_emits_at_most_one_edge_per_instance():
    """worksAt has owl:maxCardinality 1 — each Person should have ≤1 worksAt edge."""
    ttl, _ = generate_data(SAMPLE_ONTOLOGY, NS, count=10)
    g = Graph()
    g.parse(data=ttl, format="turtle")
    works_at = URIRef(NS + "worksAt")
    counts = {}
    for s, _, _ in g.triples((None, works_at, None)):
        counts[s] = counts.get(s, 0) + 1
    assert all(n == 1 for n in counts.values()), counts


def test_determinism_same_seed_same_output():
    a, _ = generate_data(SAMPLE_ONTOLOGY, NS, count=3, seed=7)
    b, _ = generate_data(SAMPLE_ONTOLOGY, NS, count=3, seed=7)
    assert a == b


def test_different_seed_different_output():
    a, _ = generate_data(SAMPLE_ONTOLOGY, NS, count=5, seed=1)
    b, _ = generate_data(SAMPLE_ONTOLOGY, NS, count=5, seed=2)
    assert a != b


def test_count_out_of_range_rejected():
    import pytest
    with pytest.raises(ValueError):
        generate_data(SAMPLE_ONTOLOGY, NS, count=0)
    with pytest.raises(ValueError):
        generate_data(SAMPLE_ONTOLOGY, NS, count=10_000)


# ── Part 3 changes: sequential ids + enum hints ───────────────────────────────

def test_short_prefix_for_common_class_names():
    from pipeline.data_generator import _short_prefix_for
    assert _short_prefix_for("WorkOrder") == "WO"
    assert _short_prefix_for("MeetingNote") == "MN"
    assert _short_prefix_for("Trip") == "TRI"
    assert _short_prefix_for("Order") == "ORD"


def test_id_properties_are_class_prefixed_sequential_strings():
    """`randint(1,1000)` for every integer id used to collide across classes
    (e.g. workOrderId=472 == actionItemId=472). Now each id-shaped property is
    a class-prefixed sequential string like 'WO-0001' — unique across the bundle."""
    ontology = """
@prefix ex:   <http://example.org/x#> .
@prefix owl:  <http://www.w3.org/2002/07/owl#> .
@prefix rdfs: <http://www.w3.org/2000/01/rdf-schema#> .
@prefix xsd:  <http://www.w3.org/2001/XMLSchema#> .

ex:Order a owl:Class .
ex:Customer a owl:Class .

ex:orderId    a owl:DatatypeProperty ; rdfs:domain ex:Order ;    rdfs:range xsd:string .
ex:customerId a owl:DatatypeProperty ; rdfs:domain ex:Customer ; rdfs:range xsd:string .
"""
    ttl, _ = generate_data(ontology, "http://example.org/x#", count=5)
    g = Graph(); g.parse(data=ttl, format="turtle")
    order_ids   = sorted(str(o) for _, _, o in g.triples((None, URIRef("http://example.org/x#orderId"), None)))
    cust_ids    = sorted(str(o) for _, _, o in g.triples((None, URIRef("http://example.org/x#customerId"), None)))
    # Per-class, 1-indexed, zero-padded; no overlap between classes.
    assert all(v.startswith("ORD-") for v in order_ids), f"orderId values: {order_ids}"
    assert all(v.startswith("CUS-") for v in cust_ids),  f"customerId values: {cust_ids}"
    assert set(order_ids).isdisjoint(cust_ids), "id values must be globally unique across classes"


def test_enum_hints_override_default_statuses():
    """When the bundle's manifest supplies enum hints, the generator uses them
    instead of the generic _STATUSES list — so filtering by the user's
    actual status values (e.g. 'APPROVED', 'CONDITIONAL') returns rows."""
    ontology = """
@prefix ex:   <http://example.org/x#> .
@prefix owl:  <http://www.w3.org/2002/07/owl#> .
@prefix rdfs: <http://www.w3.org/2000/01/rdf-schema#> .
@prefix xsd:  <http://www.w3.org/2001/XMLSchema#> .

ex:Decision a owl:Class .
ex:status a owl:DatatypeProperty ; rdfs:domain ex:Decision ; rdfs:range xsd:string .
"""
    hints = {"Decision": {"status": ["APPROVED", "CONDITIONAL", "REJECTED"]}}
    ttl, _ = generate_data(ontology, "http://example.org/x#", count=20, seed=1, enum_hints_by_class=hints)
    g = Graph(); g.parse(data=ttl, format="turtle")
    values = {str(o) for _, _, o in g.triples((None, URIRef("http://example.org/x#status"), None))}
    assert values, "no status values generated"
    assert values.issubset({"APPROVED", "CONDITIONAL", "REJECTED"}), f"got unexpected values: {values}"
