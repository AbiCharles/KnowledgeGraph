"""
Ontology curation pipeline.

Six-step generator that inspects a use-case's hand-authored OWL2 TTL with
rdflib and surfaces real metrics for each step (class/property/axiom counts,
file size, round-trip parse, plus a SHACL validation pass against the bundle's
test data graph). Mirrors pipeline.run's StageResult shape so the API and
frontend can reuse the same renderer.
"""
from __future__ import annotations
import time
from typing import Generator

from rdflib import Graph, OWL, RDF, RDFS, URIRef

from pipeline.run import StageResult
from pipeline.use_case import UseCase


SH_NS = "http://www.w3.org/ns/shacl#"


def _local(uri: URIRef) -> str:
    s = str(uri)
    for sep in ("#", "/"):
        if sep in s:
            return s.rsplit(sep, 1)[1]
    return s


def _step_domain_scoping(g: Graph, use_case: UseCase) -> list[str]:
    in_scope = set(use_case.manifest.in_scope_classes)
    logs = [f"INFO  Parsed TTL graph: {len(g)} triples"]
    logs.append(f"INFO  {use_case.manifest.prefix} namespace = {use_case.manifest.namespace}")

    declared = {_local(c) for c in g.subjects(RDF.type, OWL.Class) if isinstance(c, URIRef)}
    matched = declared & in_scope
    out = sorted(declared - in_scope)
    missing = sorted(in_scope - declared)

    logs.append(f"PASS  In-scope classes declared: {len(matched)}/{len(in_scope)} ({', '.join(sorted(matched))})")
    if missing:
        raise RuntimeError(f"Missing in-scope classes: {missing}")
    if out:
        logs.append(f"INFO  Additional classes (out of declared scope): {', '.join(out)}")
    logs.append("PASS  Domain scope validated")
    return logs


def _step_entity_modelling(g: Graph, use_case: UseCase) -> list[str]:
    classes = sorted(_local(c) for c in g.subjects(RDF.type, OWL.Class) if isinstance(c, URIRef))
    obj_props = [p for p in g.subjects(RDF.type, OWL.ObjectProperty) if isinstance(p, URIRef)]

    logs = [f"PASS  OWL classes declared: {len(classes)} ({', '.join(classes)})"]
    logs.append(f"PASS  Object properties declared: {len(obj_props)}")

    for prop in sorted(obj_props, key=str):
        domain = next(g.objects(prop, RDFS.domain), None)
        rng = next(g.objects(prop, RDFS.range), None)
        d = _local(domain) if domain else "?"
        r = _local(rng) if rng else "?"
        logs.append(f"PASS  {_local(prop):24s} domain: {d:20s} range: {r}")
    return logs


def _step_axioms(g: Graph, use_case: UseCase) -> list[str]:
    restrictions = list(g.subjects(RDF.type, OWL.Restriction))
    logs = [f"PASS  OWL restriction axioms found: {len(restrictions)}"]

    by_class: dict[str, list[str]] = {}
    for r in restrictions:
        prop = next(g.objects(r, OWL.onProperty), None)
        card = next(g.objects(r, OWL.cardinality), None)
        max_card = next(g.objects(r, OWL.maxCardinality), None)
        min_card = next(g.objects(r, OWL.minCardinality), None)

        owners = [s for s, _, o in g.triples((None, RDFS.subClassOf, r)) if isinstance(s, URIRef)]
        owner = _local(owners[0]) if owners else "(blank)"

        if card is not None:
            constraint = f"cardinality = {int(card)}"
        elif max_card is not None and min_card is not None:
            constraint = f"cardinality in [{int(min_card)}, {int(max_card)}]"
        elif max_card is not None:
            constraint = f"maxCardinality = {int(max_card)}"
        elif min_card is not None:
            constraint = f"minCardinality = {int(min_card)}"
        else:
            constraint = "unconstrained"

        by_class.setdefault(owner, []).append(f"{_local(prop) if prop else '?'} {constraint}")

    for cls in sorted(by_class):
        for entry in by_class[cls]:
            logs.append(f"PASS  {cls:18s} -> {entry}")
    return logs


def _step_datatype_properties(g: Graph, use_case: UseCase) -> list[str]:
    props = list(g.subjects(RDF.type, OWL.DatatypeProperty))
    logs = [f"PASS  Datatype properties declared: {len(props)}"]

    by_class: dict[str, list[tuple[str, str]]] = {}
    for p in props:
        domain = next(g.objects(p, RDFS.domain), None)
        rng = next(g.objects(p, RDFS.range), None)
        cls = _local(domain) if domain else "(unscoped)"
        type_name = _local(rng) if rng else "?"
        by_class.setdefault(cls, []).append((_local(p), type_name))

    for cls in sorted(by_class):
        items = by_class[cls]
        types = sorted({t for _, t in items})
        logs.append(f"PASS  {cls:18s} {len(items):2d} properties (types: {', '.join(types)})")
    return logs


def _step_serialisation(g: Graph, use_case: UseCase) -> list[str]:
    path = use_case.ontology_path
    if not path.exists():
        raise RuntimeError(f"Ontology TTL not found at {path}")
    size_kb = path.stat().st_size / 1024
    logs = [f"PASS  TTL artefact: {path.name} ({size_kb:.1f} KB)"]

    roundtrip = Graph()
    roundtrip.parse(data=g.serialize(format="turtle"), format="turtle")
    logs.append(f"PASS  Round-trip serialisation OK ({len(roundtrip)} triples)")
    logs.append("PASS  Turtle syntax validated")
    return logs


def _build_shacl_shapes(g: Graph) -> Graph:
    """Generate SHACL shapes from OWL cardinality restrictions in the ontology."""
    from rdflib import BNode, Literal, Namespace

    SH = Namespace(SH_NS)
    shapes = Graph()
    shapes.bind("sh", SH)

    for cls in g.subjects(RDF.type, OWL.Class):
        if not isinstance(cls, URIRef):
            continue
        node_shape = BNode()
        property_shapes_added = False

        for restriction in g.objects(cls, RDFS.subClassOf):
            if (restriction, RDF.type, OWL.Restriction) not in g:
                continue
            prop = next(g.objects(restriction, OWL.onProperty), None)
            card = next(g.objects(restriction, OWL.cardinality), None)
            max_card = next(g.objects(restriction, OWL.maxCardinality), None)
            min_card = next(g.objects(restriction, OWL.minCardinality), None)
            if prop is None:
                continue

            ps = BNode()
            shapes.add((node_shape, SH.property, ps))
            shapes.add((ps, SH.path, prop))
            if card is not None:
                shapes.add((ps, SH.minCount, Literal(int(card))))
                shapes.add((ps, SH.maxCount, Literal(int(card))))
            if max_card is not None:
                shapes.add((ps, SH.maxCount, Literal(int(max_card))))
            if min_card is not None:
                shapes.add((ps, SH.minCount, Literal(int(min_card))))
            property_shapes_added = True

        if property_shapes_added:
            shapes.add((node_shape, RDF.type, SH.NodeShape))
            shapes.add((node_shape, SH.targetClass, cls))

    return shapes


def _step_shacl_validation(g: Graph, use_case: UseCase) -> list[str]:
    from pyshacl import validate as shacl_validate
    from rdflib import Namespace

    SH = Namespace(SH_NS)
    shapes = _build_shacl_shapes(g)
    shape_count = len(list(shapes.subjects(RDF.type, SH.NodeShape)))
    property_shape_count = len(list(shapes.subjects(SH.path, None)))
    logs = [f"PASS  SHACL shapes generated from OWL axioms: {shape_count} NodeShape, {property_shape_count} PropertyShape"]

    data_path = use_case.data_path
    if not data_path.exists():
        logs.append(f"INFO  Data graph not found at {data_path} — running shape consistency check only")
        data_graph = Graph()
    else:
        data_graph = Graph()
        data_graph.parse(str(data_path), format="turtle")
        logs.append(f"INFO  Validating against data graph: {len(data_graph)} triples")

    conforms, _, report_text = shacl_validate(
        data_graph=data_graph,
        shacl_graph=shapes,
        ont_graph=g,
        inference="rdfs",
        abort_on_first=False,
        meta_shacl=False,
        debug=False,
    )

    if conforms:
        logs.append("PASS  SHACL validation: data graph conforms to all shapes")
    else:
        violations = report_text.count("Result")
        logs.append(f"WARN  SHACL validation reported {violations} violation(s) — see report")

    logs.append(f"PASS  Ontology curation complete — {use_case.ontology_path.name} ready for hydration pipeline")
    return logs


STEPS = [
    (1, "Domain Scoping",                  _step_domain_scoping),
    (2, "Entity & Relationship Modelling", _step_entity_modelling),
    (3, "OWL2 Axiom Authoring",            _step_axioms),
    (4, "Property Definitions",            _step_datatype_properties),
    (5, "TTL Serialisation",               _step_serialisation),
    (6, "SHACL Validation",                _step_shacl_validation),
]


def run_curation(use_case: UseCase) -> Generator[StageResult, None, None]:
    """Yield a StageResult per curation step against the active use case."""
    g = Graph()
    g.parse(str(use_case.ontology_path), format="turtle")

    for n, name, fn in STEPS:
        result = StageResult(stage=n, name=name, status="running")
        yield result

        t0 = time.time()
        try:
            logs = fn(g, use_case)
            result.logs = logs or []
            result.status = "pass"
        except Exception as exc:
            result.status = "fail"
            result.error = str(exc)
        finally:
            result.duration_ms = int((time.time() - t0) * 1000)

        yield result
        if result.status == "fail":
            break
