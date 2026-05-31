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


def _short_prefix_for(class_local: str) -> str:
    """Derive a 2-3 letter prefix from a PascalCase class name.

    Examples:
      WorkOrder   -> WO
      MeetingNote -> MN
      Order       -> ORD
      Trip        -> TR
      ""          -> X    (defensive fallback so we never return empty)
    """
    if not class_local:
        return "X"
    uppers = "".join(c for c in class_local if c.isupper())
    if len(uppers) >= 2:
        return uppers[:3]
    return class_local[:3].upper() or "X"


def _local(uri: URIRef) -> str:
    s = str(uri)
    for sep in ("#", "/"):
        if sep in s:
            return s.rsplit(sep, 1)[1]
    return s


def _datatype_value(
    prop_local: str,
    range_uri: URIRef | None,
    rng: random.Random,
    class_local: str | None = None,
    instance_index: int | None = None,
    enum_hints: list[str] | None = None,
) -> Literal:
    """Pick a value plausible for the property's range. Prop name is used as
    a hint (e.g. anything ending in 'Status' picks from STATUSES).

    When `class_local` and `instance_index` are supplied AND the property
    looks like an identifier (name ends in 'id'), emit a class-prefixed
    sequential string like 'WO-0001'. That gives readable, globally-unique
    ids without the cross-class collisions a raw randint(1, 1000) used to
    produce. Non-id integer properties keep the old randint behaviour.

    When `enum_hints` is non-empty and the property's name looks enum-ish
    (contains 'status', 'priority', or 'severity'), pick from the supplied
    hints instead of the built-in defaults so the generated data matches
    the user's intended vocabulary.
    """
    name = prop_local.lower()
    # ID-shaped string ids first — applies regardless of declared range so an
    # LLM that declared an id as xsd:integer still gets a clean string id.
    if (
        name.endswith("id")
        and class_local is not None
        and instance_index is not None
    ):
        prefix = _short_prefix_for(class_local)
        return Literal(f"{prefix}-{instance_index + 1:04d}", datatype=XSD.string)

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
        if enum_hints:
            return Literal(rng.choice(enum_hints))
        return Literal(rng.choice(_STATUSES))
    if "priority" in name or "severity" in name:
        if enum_hints:
            return Literal(rng.choice(enum_hints))
        return Literal(rng.choice(_PRIORITIES))
    if name.endswith("id"):
        # Defensive fallback for callers that don't thread class/index through.
        return Literal(f"{prop_local[:-2].upper() or 'ID'}-{rng.randint(1000, 9999)}")
    if "name" in name or "label" in name or "title" in name:
        return Literal(" ".join(rng.choice(_WORDS) for _ in range(2)).title())
    if "description" in name or "comment" in name or "note" in name:
        return Literal(" ".join(rng.choice(_WORDS) for _ in range(rng.randint(4, 8))))
    # Last resort: if enum_hints were supplied for an otherwise-generic string
    # property, prefer them over the random "alpha-123" filler.
    if enum_hints:
        return Literal(rng.choice(enum_hints))
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


def generate_data(
    ontology_ttl: str,
    bundle_ns: str,
    count: int = 10,
    seed: int = 42,
    enum_hints_by_class: dict[str, dict[str, list[str]]] | None = None,
) -> tuple[str, dict]:
    """Generate instance TTL.

    Args:
      ontology_ttl: the bundle's ontology.ttl text
      bundle_ns: the bundle's primary namespace (manifest.namespace) — instance
                 URIs are minted under this namespace so n10s SHORTEN-mode
                 prefixes resolve them with the bundle's prefix.
      count: how many instances to emit per class.
      seed: deterministic by default so the same ontology produces the same
            data on repeat runs (operators can re-roll by changing the seed).
      enum_hints_by_class: optional per-class, per-property allowed-value
            lists, e.g. ``{"Decision": {"status": ["APPROVED", "CONDITIONAL"]}}``.
            When the property name looks enum-ish (status/priority/severity)
            the generator picks from these instead of the built-in defaults
            so sample data matches the user's intended vocabulary. Caller
            (generator._build_data_ttl) typically forwards this from the
            manifest's ``sample_enum_values_hints`` field.

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

    hints_by_class = enum_hints_by_class or {}

    # Pass 1 — mint instances and attach datatype literals + label.
    instances_by_class: dict[URIRef, list[URIRef]] = {}
    summary_classes: list[dict] = []
    for cls in classes:
        local = _local(cls)
        class_hints = hints_by_class.get(local) or {}
        instances = []
        for i in range(1, count + 1):
            inst = URIRef(f"{bundle_ns}{local}_{i:03d}")
            out.add((inst, RDF.type, cls))
            out.add((inst, RDFS.label, Literal(f"{local} {i}")))
            for p in _props_for(onto, OWL.DatatypeProperty, cls, dt_props):
                ranges = list(onto.objects(p, RDFS.range))
                rng_uri = ranges[0] if ranges else None
                prop_local = _local(p)
                out.add((inst, p, _datatype_value(
                    prop_local,
                    rng_uri,
                    rng,
                    class_local=local,
                    instance_index=i - 1,
                    enum_hints=class_hints.get(prop_local),
                )))
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
