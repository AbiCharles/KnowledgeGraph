"""Programmatic edits to an OWL ontology — add class / datatype property /
object property without hand-editing the TTL.

The editor parses the current ontology with rdflib, applies the requested
mutation, and serialises back to TTL while preserving the existing prefix
bindings. Returns the new TTL plus a summary of what changed; the caller
(api/routes/use_cases.py) feeds it through register_uploaded so the swap
is atomic and the prior ontology is archived.
"""
from __future__ import annotations
import re
from typing import Literal

from rdflib import Graph, Literal as RDFLiteral, Namespace, OWL, RDF, RDFS, URIRef, XSD
from rdflib.namespace import SKOS


_NAME_RE = re.compile(r"^[A-Za-z][A-Za-z0-9_]{0,63}$")
# xsd ranges we accept on the wire — keep it to the common ones the
# generator + validator recognise.
_XSD_RANGES = {
    "string":   XSD.string,
    "integer":  XSD.integer,
    "decimal":  XSD.decimal,
    "boolean":  XSD.boolean,
    "date":     XSD.date,
    "dateTime": XSD.dateTime,
}


def _validate_local_name(kind: str, name: str) -> None:
    if not name or not _NAME_RE.match(name):
        raise ValueError(
            f"Invalid {kind} name {name!r}: must match {_NAME_RE.pattern} "
            "(starts with a letter, then letters/digits/underscores, max 64 chars)."
        )


def add_class(ontology_ttl: str, namespace: str, local_name: str, label: str | None = None,
              description: str | None = None) -> tuple[str, dict]:
    _validate_local_name("class", local_name)
    g = Graph()
    g.parse(data=ontology_ttl, format="turtle")
    NS = Namespace(namespace)
    cls = NS[local_name]
    if (cls, RDF.type, OWL.Class) in g:
        raise ValueError(f"Class {local_name!r} already exists in ontology.")
    g.add((cls, RDF.type, OWL.Class))
    g.add((cls, RDFS.label, RDFLiteral(label or local_name)))
    if description:
        g.add((cls, SKOS.definition, RDFLiteral(description)))
    return g.serialize(format="turtle"), {
        "added": "class",
        "name": local_name,
        "uri": str(cls),
    }


def add_datatype_property(ontology_ttl: str, namespace: str, local_name: str,
                          domain_class: str, xsd_range: str = "string",
                          label: str | None = None) -> tuple[str, dict]:
    _validate_local_name("datatype property", local_name)
    _validate_local_name("domain class", domain_class)
    if xsd_range not in _XSD_RANGES:
        raise ValueError(f"Unsupported xsd range {xsd_range!r}. Supported: {sorted(_XSD_RANGES)}")
    g = Graph()
    g.parse(data=ontology_ttl, format="turtle")
    NS = Namespace(namespace)
    prop = NS[local_name]
    cls = NS[domain_class]
    if (prop, RDF.type, OWL.DatatypeProperty) in g:
        raise ValueError(f"Datatype property {local_name!r} already exists.")
    if (cls, RDF.type, OWL.Class) not in g:
        raise ValueError(f"Domain class {domain_class!r} not found — add the class first.")
    g.add((prop, RDF.type, OWL.DatatypeProperty))
    g.add((prop, RDFS.label, RDFLiteral(label or local_name)))
    g.add((prop, RDFS.domain, cls))
    g.add((prop, RDFS.range, _XSD_RANGES[xsd_range]))
    return g.serialize(format="turtle"), {
        "added": "datatype_property",
        "name": local_name,
        "domain": domain_class,
        "range": xsd_range,
    }


def add_object_property(ontology_ttl: str, namespace: str, local_name: str,
                        domain_class: str, range_class: str,
                        functional: bool = False,
                        label: str | None = None) -> tuple[str, dict]:
    _validate_local_name("object property", local_name)
    _validate_local_name("domain class", domain_class)
    _validate_local_name("range class", range_class)
    g = Graph()
    g.parse(data=ontology_ttl, format="turtle")
    NS = Namespace(namespace)
    prop = NS[local_name]
    dom = NS[domain_class]
    rng = NS[range_class]
    if (prop, RDF.type, OWL.ObjectProperty) in g:
        raise ValueError(f"Object property {local_name!r} already exists.")
    for c, label_ in ((dom, "Domain"), (rng, "Range")):
        if (c, RDF.type, OWL.Class) not in g:
            raise ValueError(f"{label_} class {c.split('#')[-1]!r} not found — add the class first.")
    g.add((prop, RDF.type, OWL.ObjectProperty))
    g.add((prop, RDFS.label, RDFLiteral(label or local_name)))
    g.add((prop, RDFS.domain, dom))
    g.add((prop, RDFS.range, rng))
    if functional:
        g.add((prop, RDF.type, OWL.FunctionalProperty))
    return g.serialize(format="turtle"), {
        "added": "object_property",
        "name": local_name,
        "domain": domain_class,
        "range": range_class,
        "functional": functional,
    }


def apply_edit(ontology_ttl: str, namespace: str, edit: dict) -> tuple[str, dict]:
    """Dispatcher: routes to the appropriate add_* helper based on edit['kind']."""
    kind = edit.get("kind")
    if kind == "class":
        return add_class(ontology_ttl, namespace,
                         local_name=edit.get("name", ""),
                         label=edit.get("label"),
                         description=edit.get("description"))
    if kind == "datatype_property":
        return add_datatype_property(ontology_ttl, namespace,
                                     local_name=edit.get("name", ""),
                                     domain_class=edit.get("domain", ""),
                                     xsd_range=edit.get("range", "string"),
                                     label=edit.get("label"))
    if kind == "object_property":
        return add_object_property(ontology_ttl, namespace,
                                   local_name=edit.get("name", ""),
                                   domain_class=edit.get("domain", ""),
                                   range_class=edit.get("range", ""),
                                   functional=bool(edit.get("functional")),
                                   label=edit.get("label"))
    raise ValueError(f"Unknown edit kind {kind!r}; expected class | datatype_property | object_property")
