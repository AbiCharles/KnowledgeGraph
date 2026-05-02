"""Structural diff between two snapshot bundles."""
from pipeline.manifest_diff import diff_snapshots


ONT_BASE = """\
@prefix ex:   <http://example.org/test#> .
@prefix owl:  <http://www.w3.org/2002/07/owl#> .
@prefix rdfs: <http://www.w3.org/2000/01/rdf-schema#> .

ex:Person  a owl:Class .
ex:Company a owl:Class .
ex:worksAt a owl:ObjectProperty ; rdfs:domain ex:Person ; rdfs:range ex:Company .
"""

ONT_EVOLVED = """\
@prefix ex:   <http://example.org/test#> .
@prefix owl:  <http://www.w3.org/2002/07/owl#> .
@prefix rdfs: <http://www.w3.org/2000/01/rdf-schema#> .

ex:Person     a owl:Class .
ex:Department a owl:Class .
ex:worksAt   a owl:ObjectProperty ; rdfs:domain ex:Person ; rdfs:range ex:Department .
ex:reportsTo a owl:ObjectProperty ; rdfs:domain ex:Person ; rdfs:range ex:Person .
"""


def test_topology_diff_classifies_added_removed_common():
    d = diff_snapshots(
        {"ontology": ONT_BASE, "manifest": "", "data": ""},
        {"ontology": ONT_EVOLVED, "manifest": "", "data": ""},
    )
    topo = d["ontology"]["topology"]
    by_name = {n["name"]: n["status"] for n in topo["nodes"]}
    assert by_name == {"Person": "common", "Company": "removed", "Department": "added"}

    edge_status = {(e["domain"], e["property"], e["range"]): e["status"] for e in topo["edges"]}
    # Same property name worksAt but the range changed → both old and new edges show.
    assert edge_status[("Person", "worksAt", "Company")] == "removed"
    assert edge_status[("Person", "worksAt", "Department")] == "added"
    assert edge_status[("Person", "reportsTo", "Person")] == "added"


def test_topology_diff_handles_empty_old():
    d = diff_snapshots(
        {"ontology": "", "manifest": "", "data": ""},
        {"ontology": ONT_BASE, "manifest": "", "data": ""},
    )
    topo = d["ontology"]["topology"]
    assert all(n["status"] == "added" for n in topo["nodes"])
    assert all(e["status"] == "added" for e in topo["edges"])


def test_topology_diff_handles_unchanged_ontology():
    d = diff_snapshots(
        {"ontology": ONT_BASE, "manifest": "", "data": ""},
        {"ontology": ONT_BASE, "manifest": "", "data": ""},
    )
    topo = d["ontology"]["topology"]
    assert all(n["status"] == "common" for n in topo["nodes"])
    assert all(e["status"] == "common" for e in topo["edges"])
