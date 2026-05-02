"""Synthesise plausible instance data from an OWL ontology.

Given an ontology TTL the generator walks each `owl:Class` and emits
`count` instances per class. Each instance gets:
  - rdf:type → its class
  - rdfs:label / skos:prefLabel → "<ClassLocalName> <n>"
  - One literal per `owl:DatatypeProperty` whose domain is the class
    (or whose domain is unspecified — we default-include those), typed
    by the property's `rdfs:range` (xsd:string|integer|date|boolean|...).
  - One or more object-property edges per `owl:ObjectProperty` whose
    domain is the class, picking a random instance of the declared range.
    `owl:cardinality 1` and `owl:maxCardinality 1` are honoured.

The generator never modifies the ontology graph itself — it returns a
TTL string of pure instance data, suitable for writing to data.ttl. URIs
use the bundle's primary namespace (`bundle_ns`) so n10s namespace mapping
keeps working with the existing prefix.
"""
from __future__ import annotations
import random
from datetime import date, timedelta
from typing import Iterable

from rdflib import Graph, Literal, Namespace, OWL, RDF, RDFS, URIRef, XSD
from rdflib.namespace import SKOS


# Plausible literal vocabularies — small but enough to make a generated
# graph readable in the visualiser without pulling in faker.
_WORDS = [
    "alpha", "beta", "gamma", "delta", "echo", "foxtrot", "golf", "hotel",
    "india", "juliet", "kilo", "lima", "mike", "november", "oscar", "papa",
    "quebec", "romeo", "sierra", "tango", "uniform", "victor", "whiskey",
    "xray", "yankee", "zulu",
]
_STATUSES = ["OPEN", "IN_PROGRESS", "CLOSED", "BLOCKED", "REVIEW"]
_PRIORITIES = ["LOW", "MEDIUM", "HIGH", "URGENT"]


def _local(uri: URIRef) -> str:
    s = str(uri)
    for sep in ("#", "/"):
        if sep in s:
            return s.rsplit(sep, 1)[1]
    return s


def _datatype_value(prop_local: str, range_uri: URIRef | None, rng: random.Random) -> Literal:
    """Pick a value plausible for the property's range. Prop name is used as
    a hint (e.g. anything ending in 'Status' picks from STATUSES)."""
    name = prop_local.lower()
    if range_uri == XSD.integer or range_uri == XSD.int or range_uri == XSD.long or range_uri == XSD.nonNegativeInteger:
        return Literal(rng.randint(1, 1000), datatype=XSD.integer)
    if range_uri == XSD.decimal or range_uri == XSD.double or range_uri == XSD.float:
        return Literal(round(rng.uniform(0, 1000), 2), datatype=XSD.decimal)
    if range_uri == XSD.boolean:
        return Literal(rng.choice([True, False]))
    if range_uri == XSD.date:
        d = date.today() - timedelta(days=rng.randint(0, 365))
        return Literal(d.isoformat(), datatype=XSD.date)
    if range_uri == XSD.dateTime:
        d = date.today() - timedelta(days=rng.randint(0, 365))
        return Literal(d.isoformat() + "T00:00:00", datatype=XSD.dateTime)
    # String-ish — try to be cute about common property names so the demo
    # data reads naturally instead of being all "alpha-12 beta-3".
    if "status" in name:
        return Literal(rng.choice(_STATUSES))
    if "priority" in name or "severity" in name:
        return Literal(rng.choice(_PRIORITIES))
    if name.endswith("id"):
        return Literal(f"{prop_local[:-2].upper() or 'ID'}-{rng.randint(1000, 9999)}")
    if "name" in name or "label" in name or "title" in name:
        return Literal(" ".join(rng.choice(_WORDS) for _ in range(2)).title())
    if "description" in name or "comment" in name or "note" in name:
        return Literal(" ".join(rng.choice(_WORDS) for _ in range(rng.randint(4, 8))))
    return Literal(rng.choice(_WORDS) + "-" + str(rng.randint(1, 999)))


def _classes(g: Graph) -> list[URIRef]:
    return sorted(
        (c for c in g.subjects(RDF.type, OWL.Class) if isinstance(c, URIRef)),
        key=str,
    )


def _props_for(g: Graph, prop_type: URIRef, cls: URIRef, all_props: list[URIRef]) -> Iterable[URIRef]:
    """Return properties of `prop_type` whose declared domain is `cls`. If a
    property has no domain at all we fall back to attaching it to every class
    so under-specified ontologies still produce data — this matches the
    permissive spirit of OWL open-world."""
    for p in all_props:
        domains = list(g.objects(p, RDFS.domain))
        if not domains:
            yield p
        elif cls in domains:
            yield p


def _max_card(g: Graph, prop: URIRef) -> int | None:
    """Return the max-cardinality if declared (1 means functional)."""
    for o in g.objects(prop, OWL.cardinality):
        try:
            return int(o)
        except Exception:
            pass
    for o in g.objects(prop, OWL.maxCardinality):
        try:
            return int(o)
        except Exception:
            pass
    if (prop, RDF.type, OWL.FunctionalProperty) in g:
        return 1
    return None


def generate_data(ontology_ttl: str, bundle_ns: str, count: int = 10, seed: int = 42) -> tuple[str, dict]:
    """Generate instance TTL.

    Args:
      ontology_ttl: the bundle's ontology.ttl text
      bundle_ns: the bundle's primary namespace (manifest.namespace) — instance
                 URIs are minted under this namespace so n10s SHORTEN-mode
                 prefixes resolve them with the bundle's prefix.
      count: how many instances to emit per class.
      seed: deterministic by default so the same ontology produces the same
            data on repeat runs (operators can re-roll by changing the seed).

    Returns: (ttl_text, summary_dict). The summary lists per-class counts and
    total nodes/edges so the caller can surface it in the UI.
    """
    if count < 1 or count > 500:
        raise ValueError(f"count must be between 1 and 500, got {count}")

    onto = Graph()
    onto.parse(data=ontology_ttl, format="turtle")

    classes = _classes(onto)
    if not classes:
        return "", {"classes": [], "total_nodes": 0, "total_edges": 0}

    rng = random.Random(seed)
    INST = Namespace(bundle_ns)
    out = Graph()
    out.bind("", INST)
    out.bind("rdfs", RDFS)
    out.bind("skos", SKOS)
    out.bind("xsd", XSD)
    # Bring the ontology's own prefixes through so the generated TTL reads
    # naturally (kf-mfg:WorkOrder rather than ns1:WorkOrder).
    for prefix, uri in onto.namespaces():
        if prefix:
            out.bind(prefix, uri, replace=True)

    obj_props = sorted(
        (p for p in onto.subjects(RDF.type, OWL.ObjectProperty) if isinstance(p, URIRef)),
        key=str,
    )
    dt_props = sorted(
        (p for p in onto.subjects(RDF.type, OWL.DatatypeProperty) if isinstance(p, URIRef)),
        key=str,
    )

    # Pass 1 — mint instances and attach datatype literals + label.
    instances_by_class: dict[URIRef, list[URIRef]] = {}
    summary_classes: list[dict] = []
    for cls in classes:
        local = _local(cls)
        instances = []
        for i in range(1, count + 1):
            inst = URIRef(f"{bundle_ns}{local}_{i:03d}")
            out.add((inst, RDF.type, cls))
            out.add((inst, RDFS.label, Literal(f"{local} {i}")))
            for p in _props_for(onto, OWL.DatatypeProperty, cls, dt_props):
                ranges = list(onto.objects(p, RDFS.range))
                rng_uri = ranges[0] if ranges else None
                out.add((inst, p, _datatype_value(_local(p), rng_uri, rng)))
            instances.append(inst)
        instances_by_class[cls] = instances
        summary_classes.append({"class": local, "count": len(instances)})

    # Pass 2 — wire object properties. For each object property whose domain
    # is `cls`, link each `cls` instance to a random instance of the declared
    # range. Functional/max-card-1 properties get one edge; others get 1-3.
    edge_count = 0
    for cls in classes:
        for p in _props_for(onto, OWL.ObjectProperty, cls, obj_props):
            ranges = [r for r in onto.objects(p, RDFS.range) if isinstance(r, URIRef)]
            if not ranges:
                continue
            target_class = ranges[0]
            targets = instances_by_class.get(target_class, [])
            if not targets:
                continue
            max_c = _max_card(onto, p)
            for src in instances_by_class[cls]:
                k = 1 if max_c == 1 else rng.randint(1, min(3, len(targets)))
                for tgt in rng.sample(targets, k):
                    out.add((src, p, tgt))
                    edge_count += 1

    ttl = out.serialize(format="turtle")
    summary = {
        "classes": summary_classes,
        "total_nodes": sum(len(v) for v in instances_by_class.values()),
        "total_edges": edge_count,
    }
    return ttl, summary
