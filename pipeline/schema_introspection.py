"""Build an LLM-friendly schema description from a use case's ontology TTL.

Used by both api/routes/nl.py (NL→Cypher prompt) and agents/dynamic.py
(per-agent system-prompt schema injection) so the two stay in sync and there's
no manufacturing-domain bias when running on a different bundle.
"""
from __future__ import annotations
from rdflib import Graph, OWL, RDF, RDFS, URIRef

from pipeline.use_case import UseCase


def _local(uri: URIRef) -> str:
    s = str(uri)
    for sep in ("#", "/"):
        if sep in s:
            return s.rsplit(sep, 1)[1]
    return s


def schema_description(use_case: UseCase) -> str:
    """Return a markdown-style schema sketch suitable for LLM context.

    Lists every OWL class with its datatype properties, then every object
    property with prefixed name, domain, range. Always uses the active
    bundle's prefix so generated Cypher matches the live Neo4j labels.
    """
    g = Graph()
    g.parse(str(use_case.ontology_path), format="turtle")
    prefix = use_case.manifest.prefix

    classes = sorted({_local(c) for c in g.subjects(RDF.type, OWL.Class) if isinstance(c, URIRef)})
    obj_props = []
    for p in sorted(g.subjects(RDF.type, OWL.ObjectProperty), key=str):
        if not isinstance(p, URIRef):
            continue
        d = next(g.objects(p, RDFS.domain), None)
        r = next(g.objects(p, RDFS.range), None)
        obj_props.append((_local(p), _local(d) if d else "?", _local(r) if r else "?"))
    dt_props: dict[str, list[str]] = {}
    for p in g.subjects(RDF.type, OWL.DatatypeProperty):
        if not isinstance(p, URIRef):
            continue
        d = next(g.objects(p, RDFS.domain), None)
        cls = _local(d) if d else "(unscoped)"
        dt_props.setdefault(cls, []).append(_local(p))

    lines = [
        f"Knowledge graph schema for use case: {use_case.manifest.name}",
        f"All Neo4j labels and properties are prefixed with `{prefix}__`.",
        "",
        "Node classes with their datatype properties:",
    ]
    for cls in classes:
        props = sorted(dt_props.get(cls, []))
        lines.append(f"- {cls}  ({', '.join(props) if props else 'no properties'})")
    lines.append("")
    lines.append("Relationship types (always backtick-quote and prefix):")
    for name, dom, rng in obj_props:
        lines.append(f"- `{prefix}__{name}`  {dom} -> {rng}")
    lines.append("")
    lines.append("Cypher rules for this graph:")
    lines.append(f"- Backtick-quote labels, properties, and relationship types: `{prefix}__SomeClass`, `{prefix}__someProperty`.")
    lines.append(f"- Relationship types MUST include the `{prefix}__` prefix.")
    lines.append("- Always alias RETURN columns with AS.")
    lines.append("- Read-only only: MATCH, OPTIONAL MATCH, WITH, WHERE, RETURN, ORDER BY, LIMIT.")
    return "\n".join(lines)
