"""Build an LLM-friendly schema description from a use case's ontology TTL.

Used by both api/routes/nl.py (NL→Cypher prompt) and agents/dynamic.py
(per-agent system-prompt schema injection) so the two stay in sync and there's
no manufacturing-domain bias when running on a different bundle.

Also samples enum-shaped property values from live Neo4j so the LLM picks
correct literals (e.g. 'URGENT' not 'urgent', 'PREVENTIVE' not 'preventive').

Top-level `schema_description(use_case)` is cached on
(slug, ontology_mtime_ns, data_mtime_ns) so agents and the NL endpoint don't
pay the rdflib parse + N enum-sample queries on every call. The data mtime
is part of the key so a pipeline re-run (which rewrites data via n10s)
invalidates stale enum literals — call `invalidate_schema_cache()` to nuke
the cache explicitly (e.g. after a bundle delete).
"""
from __future__ import annotations
import logging
import re
from functools import lru_cache

from rdflib import Graph, OWL, RDF, RDFS, URIRef

from pipeline.use_case import UseCase


log = logging.getLogger(__name__)

# Property names that typically carry enum-shaped string values worth surfacing
# to the LLM. Matched as a suffix on the local name (case-insensitive). Trimmed
# from a broader earlier set — `category` and `kind` and `mode` were dropped
# because they sometimes carry free-text or PII-adjacent values; `protocol`
# kept because it's almost always a short technical token (REST-JSON etc.).
_ENUM_SUFFIX = re.compile(r"(status|type|priority|severity|state|protocol)$", re.IGNORECASE)

# Cap the number of distinct values shown per property — keeps the prompt
# focused and avoids leaking high-cardinality fields.
_MAX_VALUES_PER_PROP = 10
# Skip a property entirely if any sampled value exceeds this length. A real
# enum value is short (URGENT, PREVENTIVE, etc.); long values almost always
# mean the field carries free-text or descriptions that we don't want to ship
# to the LLM. Defence against accidental PII leaks via OpenAI prompts.
_MAX_VALUE_CHAR_LEN = 32


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

    Cached on (slug, ontology mtime, data mtime). Pipeline reruns rewrite
    data, so the data mtime in the key invalidates stale enum samples; an
    edit to the TTL invalidates structural classes/properties.
    """
    o_mtime = use_case.ontology_path.stat().st_mtime_ns if use_case.ontology_path.exists() else 0
    d_mtime = use_case.data_path.stat().st_mtime_ns if use_case.data_path.exists() else 0
    return _schema_description_cached(use_case.slug, o_mtime, d_mtime)


def invalidate_schema_cache() -> None:
    """Drop every cached schema description. Call after destructive ops
    (bundle delete, manual TTL replacement, etc.) where the (slug, mtime)
    key would otherwise serve a stale entry."""
    _schema_description_cached.cache_clear()
    _schema_summary_cached.cache_clear()


def schema_summary(use_case: UseCase) -> dict:
    """Return a structured schema for the Cypher editor's autocomplete.

    Shape:
      {
        prefix: "kf-mfg",
        labels: ["WorkOrder", "Equipment", ...],
        relationship_types: ["assignedToEquipment", ...],
        properties_by_label: {"WorkOrder": ["woStatus", "woType", ...], ...},
      }

    Same caching contract as schema_description() — cached on
    (slug, ontology_mtime). No live Neo4j hit (autocomplete latency budget
    is tight; we trust the ontology to be authoritative).
    """
    o_mtime = use_case.ontology_path.stat().st_mtime_ns if use_case.ontology_path.exists() else 0
    return _schema_summary_cached(use_case.slug, o_mtime)


@lru_cache(maxsize=16)
def _schema_summary_cached(slug: str, ontology_mtime_ns: int) -> dict:
    from pipeline.use_case_registry import load
    use_case = load(slug)
    g = Graph()
    g.parse(str(use_case.ontology_path), format="turtle")
    labels = sorted({_local(c) for c in g.subjects(RDF.type, OWL.Class) if isinstance(c, URIRef)})
    rel_types = sorted({_local(p) for p in g.subjects(RDF.type, OWL.ObjectProperty) if isinstance(p, URIRef)})
    props_by_label: dict[str, list[str]] = {l: [] for l in labels}
    unscoped: list[str] = []
    for p in g.subjects(RDF.type, OWL.DatatypeProperty):
        if not isinstance(p, URIRef):
            continue
        name = _local(p)
        domain = next(g.objects(p, RDFS.domain), None)
        if domain and isinstance(domain, URIRef):
            cls = _local(domain)
            props_by_label.setdefault(cls, []).append(name)
        else:
            unscoped.append(name)
    # Unscoped properties are surfaced under every label so the autocompleter
    # still suggests them in any MATCH context.
    if unscoped:
        for l in labels:
            props_by_label[l].extend(unscoped)
    for l in props_by_label:
        props_by_label[l] = sorted(set(props_by_label[l]))
    return {
        "prefix": use_case.manifest.prefix,
        "labels": labels,
        "relationship_types": rel_types,
        "properties_by_label": props_by_label,
    }


@lru_cache(maxsize=16)
def _schema_description_cached(slug: str, ontology_mtime_ns: int, data_mtime_ns: int) -> str:
    """Inner cached worker. Re-loads the use case from the registry so the
    cache key fully captures inputs (loading a fresh UseCase here is cheap
    compared to the rdflib parse + enum-sample queries we'd otherwise repeat)."""
    from pipeline.use_case_registry import load
    use_case = load(slug)
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

    # Print prefixed names everywhere so the LLM can literally copy what it
    # sees into Cypher. Telling it the rule once at the top and then listing
    # unprefixed names ("noteId") used to make the model emit `n.noteId`,
    # which is null in Neo4j because the real property is `mt__noteId`.
    pfx = f"{prefix}__"
    lines = [
        f"Knowledge graph schema for use case: {use_case.manifest.name}",
        f"All Neo4j labels, properties, and relationship types are prefixed "
        f"with `{pfx}` — use the exact backtick-quoted names below.",
        "",
        "Node classes with their datatype properties:",
    ]
    for cls in classes:
        props = sorted(dt_props.get(cls, []))
        prop_str = ", ".join(f"`{pfx}{p}`" for p in props) if props else "no properties"
        lines.append(f"- `{pfx}{cls}`  ({prop_str})")
    lines.append("")
    lines.append("Relationship types — each is a DIRECTED edge stored only in the "
                 "FROM → TO direction. Copy the exact Cypher pattern; reversing the "
                 "arrow returns zero rows. The English gloss in italics tells you "
                 "which class does the acting and which is acted on.")
    for name, dom, rng in obj_props:
        dom_s = f"`{pfx}{dom}`" if dom != "?" else "?"
        rng_s = f"`{pfx}{rng}`" if rng != "?" else "?"
        # Humanise the relationship name to a verb phrase; strip a trailing
        # class-name suffix when it matches the range, since the Lumen-style
        # 'capturesDecision' embeds the range and reads better as 'captures'.
        verb_src = name
        if rng != "?" and name.lower().endswith(rng.lower()) and len(name) > len(rng):
            verb_src = name[:-len(rng)]
        verb = re.sub(r"([a-z0-9])([A-Z])", r"\1 \2", verb_src).lower().strip()
        gloss = f'"{dom} {verb} {rng}"' if dom != "?" and rng != "?" else ""
        suffix = f"  — {gloss} (from left → right)" if gloss else ""
        lines.append(f"- `(:{dom_s})-[:`{pfx}{name}`]->(:{rng_s})`{suffix}")

    samples = _sample_enum_values(use_case, classes, dt_props)
    if samples:
        lines.append("")
        lines.append("Known property values (sampled from live data — use these literals exactly, including case):")
        for (cls, prop), values in samples.items():
            shown = ", ".join(repr(v) for v in values)
            lines.append(f"- `{pfx}{cls}`.`{pfx}{prop}`: {shown}")

    lines.append("")
    lines.append("Cypher rules for this graph:")
    lines.append(f"- Every label, property, and relationship type MUST be backtick-quoted "
                 f"and prefixed with `{pfx}` exactly as listed above — e.g. "
                 f"`(n:`{pfx}SomeClass`) RETURN n.`{pfx}someProperty``.")
    lines.append("- Use ONLY the relationship types listed above, in the direction shown "
                 "(`domain -> range`). NEVER invent a relationship or use one where the "
                 "domain/range classes don't match. If two classes aren't directly connected, "
                 "traverse through intermediate classes via multiple hops — e.g. "
                 f"`(a:`{pfx}A`)-[:`{pfx}r1`]->(:`{pfx}B`)-[:`{pfx}r2`]->(c:`{pfx}C`)`.")
    lines.append("- Relationship patterns MUST include the direction arrow AND a trailing "
                 "dash on both sides. Forward: `(a)-[:`rel`]->(b)`. Reverse: "
                 "`(a)<-[:`rel`]-(b)`. The patterns `(a)-[:`rel`](b)` and "
                 "`(a)<-[:`rel`](b)` are invalid Cypher.")
    lines.append("- STRONGLY PREFER forward arrows `-[:rel]->` and copy each step from the "
                 "relationship list verbatim. Only use a reverse arrow `<-[:rel]-` when the "
                 "path genuinely needs to traverse from range back to domain. Never reverse a "
                 "step just to make the path connect — re-pick a path whose listed direction "
                 "already matches the way you're walking.")
    lines.append("- When counting via multi-hop paths, use `COUNT(DISTINCT x)` to avoid "
                 "double-counting (a meeting reaches the same owner along several paths).")
    lines.append(f"- To enable the graph visualisation, prefer returning the node itself "
                 f"alongside scalars: `RETURN n.`{pfx}someProperty` AS x, n` instead of just `x`.")
    lines.append("- Always alias RETURN columns with AS.")
    lines.append("- Read-only only: MATCH, OPTIONAL MATCH, WITH, WHERE, RETURN, ORDER BY, LIMIT.")
    lines.append("- Match enum-style property values literally (e.g. 'URGENT' not 'urgent').")
    return "\n".join(lines)


def _sample_enum_values(use_case: UseCase, classes: list[str], dt_props: dict[str, list[str]]):
    """Query live Neo4j for the distinct values of properties whose names look
    like enums (Status, Type, Priority, etc.) so the LLM uses correct literals.

    Returns an ordered dict-like mapping (class, prop_local_name) -> [values].
    Failures are logged and skipped — schema description still emits structure.

    Privacy:
      * Manifest field `sample_enum_values: false` opts out entirely.
      * Properties whose sampled values include any string longer than
        _MAX_VALUE_CHAR_LEN are silently skipped (signal: free-text / PII risk).
      * Non-string sampled values are also skipped (mixed-type fields).
    """
    if not use_case.manifest.sample_enum_values:
        return {}

    # Imported locally to avoid forcing a Neo4j connection at module import
    # time (e.g. during test collection).
    from db import run_query

    samples: dict[tuple[str, str], list] = {}
    for cls in classes:
        label = use_case.label(cls)
        for prop in dt_props.get(cls, []):
            if not _ENUM_SUFFIX.search(prop):
                continue
            db_prop = use_case.prop(prop)
            try:
                rows = run_query(
                    f"MATCH (n:`{label}`) WHERE n.`{db_prop}` IS NOT NULL "
                    f"RETURN DISTINCT n.`{db_prop}` AS v LIMIT {_MAX_VALUES_PER_PROP + 1}"
                )
            except Exception as exc:
                log.warning("schema sample failed for %s.%s: %s", cls, prop, exc)
                continue
            values = [r["v"] for r in rows if r.get("v") not in (None, "")]
            if not values:
                continue
            # Privacy guard: skip the whole property if any value smells like
            # free-text (long string) or isn't a plain string.
            if any(not isinstance(v, str) or len(v) > _MAX_VALUE_CHAR_LEN for v in values):
                log.debug("skipping enum sample for %s.%s — value(s) non-string or too long", cls, prop)
                continue
            # Drop the last one as a "and more…" hint if we hit the cap.
            if len(values) > _MAX_VALUES_PER_PROP:
                values = values[:_MAX_VALUES_PER_PROP] + ["…"]
            samples[(cls, prop)] = values
    return samples
