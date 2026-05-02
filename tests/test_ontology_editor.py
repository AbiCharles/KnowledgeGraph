"""Inline ontology editor — add class / datatype prop / object prop."""
from rdflib import Graph, OWL, RDF, RDFS, URIRef, XSD

from pipeline.ontology_editor import (
    add_class, add_datatype_property, add_object_property, apply_edit,
)
import pytest


BASE_TTL = """\
@prefix ex:   <http://example.org/test#> .
@prefix owl:  <http://www.w3.org/2002/07/owl#> .
@prefix rdfs: <http://www.w3.org/2000/01/rdf-schema#> .

ex:Person a owl:Class ; rdfs:label "Person" .
"""
NS = "http://example.org/test#"


def _parse(ttl):
    g = Graph(); g.parse(data=ttl, format="turtle"); return g


def test_add_class_appends_owl_class():
    new_ttl, summary = add_class(BASE_TTL, NS, "Company", description="A business entity")
    assert summary["added"] == "class"
    g = _parse(new_ttl)
    assert (URIRef(NS + "Company"), RDF.type, OWL.Class) in g
    # Pre-existing class still present.
    assert (URIRef(NS + "Person"), RDF.type, OWL.Class) in g


def test_add_class_rejects_duplicate():
    with pytest.raises(ValueError):
        add_class(BASE_TTL, NS, "Person")


def test_add_class_rejects_bad_name():
    with pytest.raises(ValueError):
        add_class(BASE_TTL, NS, "1Bad")  # starts with digit
    with pytest.raises(ValueError):
        add_class(BASE_TTL, NS, "")


def test_add_datatype_property_with_xsd_range():
    new_ttl, summary = add_datatype_property(BASE_TTL, NS, "fullName", "Person", "string")
    assert summary == {"added": "datatype_property", "name": "fullName", "domain": "Person", "range": "string"}
    g = _parse(new_ttl)
    p = URIRef(NS + "fullName")
    assert (p, RDF.type, OWL.DatatypeProperty) in g
    assert (p, RDFS.domain, URIRef(NS + "Person")) in g
    assert (p, RDFS.range, XSD.string) in g


def test_add_datatype_property_rejects_unknown_domain():
    with pytest.raises(ValueError):
        add_datatype_property(BASE_TTL, NS, "salary", "MissingClass", "integer")


def test_add_datatype_property_rejects_unsupported_xsd():
    with pytest.raises(ValueError):
        add_datatype_property(BASE_TTL, NS, "thing", "Person", "duration")


def test_add_object_property_links_two_classes():
    ttl_with_company, _ = add_class(BASE_TTL, NS, "Company")
    new_ttl, summary = add_object_property(ttl_with_company, NS, "worksAt", "Person", "Company", functional=True)
    assert summary["functional"] is True
    g = _parse(new_ttl)
    p = URIRef(NS + "worksAt")
    assert (p, RDF.type, OWL.ObjectProperty) in g
    assert (p, RDFS.domain, URIRef(NS + "Person")) in g
    assert (p, RDFS.range, URIRef(NS + "Company")) in g
    assert (p, RDF.type, OWL.FunctionalProperty) in g


def test_add_object_property_rejects_unknown_range():
    with pytest.raises(ValueError):
        add_object_property(BASE_TTL, NS, "worksAt", "Person", "MissingClass")


def test_apply_edit_dispatcher():
    ttl, _ = apply_edit(BASE_TTL, NS, {"kind": "class", "name": "Vehicle"})
    g = _parse(ttl)
    assert (URIRef(NS + "Vehicle"), RDF.type, OWL.Class) in g

    with pytest.raises(ValueError):
        apply_edit(BASE_TTL, NS, {"kind": "garbage"})
