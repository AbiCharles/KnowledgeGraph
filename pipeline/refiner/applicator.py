"""Apply a single linter / LLM-coach finding's fix to a bundle.

Each fix.kind dispatches to a specific mutation. All paths funnel
through use_case_registry.register_uploaded so the prior version of
the bundle is auto-archived under <slug>.versions/ — every Apply
click is one Versions-panel click from rollback.

Supported fix.kind values (mirror the docstring in __init__.py):
  add_label              — adds rdfs:label triple
  add_description        — adds skos:definition triple
  add_class              — wraps ontology_editor.add_class
  add_datatype_property  — wraps ontology_editor.add_datatype_property
  add_object_property    — wraps ontology_editor.add_object_property
  convert_to_object      — converts a datatype property into an object
                            property (removes the old, adds new + edges)
  noop                   — finding has no automatic fix; raises so the
                            caller can return a clear error to the UI.
"""
from __future__ import annotations

from rdflib import Graph, Literal, OWL, RDF, RDFS, URIRef, Namespace
from rdflib.namespace import SKOS

from pipeline import use_case_registry


def apply_fix_to_text(ontology_ttl: str, namespace: str, fix: dict) -> tuple[str, dict]:
    """In-memory variant of apply_fix — operates on raw TTL text + namespace
    without touching any bundle on disk. Used by the Builder's Preview step
    so the operator can apply fixes BEFORE clicking Create. Returns
    (new_ttl, summary).

    Same fix.kind dispatch as the on-disk path; no register_uploaded
    side-effects (no auto-archive — caller manages persistence)."""
    if not isinstance(fix, dict) or "kind" not in fix:
        raise ValueError("fix must be a dict with a 'kind' key.")
    kind = fix["kind"]
    handler = _HANDLERS.get(kind)
    if handler is None:
        raise ValueError(
            f"Unknown fix kind {kind!r}. Supported: {sorted(_HANDLERS)}."
        )
    new_ontology, summary = handler(ontology_ttl, namespace, fix)
    return new_ontology, summary


def apply_fix(slug: str, fix: dict) -> dict:
    """Apply ONE fix to bundle `slug`. Returns a summary dict the route
    can pass back to the UI. Raises ValueError on bad input or noop
    fixes; the route translates that into a 400."""
    if not isinstance(fix, dict) or "kind" not in fix:
        raise ValueError("fix must be a dict with a 'kind' key.")

    bundle_dir = use_case_registry.USE_CASES_DIR / slug
    if not bundle_dir.is_dir():
        raise FileNotFoundError(f"No bundle {slug!r}")

    uc = use_case_registry.load(slug)
    ontology_text = (bundle_dir / "ontology.ttl").read_text(encoding="utf-8")
    data_text     = (bundle_dir / "data.ttl").read_text(encoding="utf-8")
    manifest_text = (bundle_dir / "manifest.yaml").read_text(encoding="utf-8")

    kind = fix["kind"]
    handler = _HANDLERS.get(kind)
    if handler is None:
        raise ValueError(
            f"Unknown fix kind {kind!r}. Supported: {sorted(_HANDLERS)}."
        )

    new_ontology, summary = handler(ontology_text, uc.manifest.namespace, fix)

    # Atomic write — same path as the inline ontology editor.
    use_case_registry.register_uploaded(
        slug,
        new_ontology.encode("utf-8"),
        data_text.encode("utf-8"),
        manifest_text.encode("utf-8"),
    )

    # Bust the schema cache so autocomplete + agent prompts pick up the
    # new label/property on the next request.
    try:
        from pipeline.schema_introspection import invalidate_schema_cache
        invalidate_schema_cache()
    except Exception:
        pass

    return {"applied": kind, "summary": summary}


# ── Per-fix handlers ────────────────────────────────────────────────────────

def _resolve_target(target: str) -> tuple[str, str]:
    """Parse 'class:Foo' or 'property:bar' into (kind, name)."""
    if ":" not in target:
        raise ValueError(f"target {target!r} must be in 'class:NAME' or 'property:NAME' form")
    kind, _, name = target.partition(":")
    if kind not in ("class", "property"):
        raise ValueError(f"target kind must be 'class' or 'property', got {kind!r}")
    if not name:
        raise ValueError("target name cannot be empty")
    return kind, name


def _add_triple(ontology_ttl: str, namespace: str, target: str, predicate, value):
    """Parse → add (subject, predicate, value) triple → re-serialise."""
    target_kind, target_name = _resolve_target(target)
    g = Graph()
    g.parse(data=ontology_ttl, format="turtle")
    subj = URIRef(namespace + target_name)
    expected_type = OWL.Class if target_kind == "class" else None
    if expected_type and (subj, RDF.type, expected_type) not in g:
        # Property fix — accept either DatatypeProperty or ObjectProperty.
        pass
    if target_kind == "property":
        if (subj, RDF.type, OWL.DatatypeProperty) not in g and (subj, RDF.type, OWL.ObjectProperty) not in g:
            raise ValueError(f"property {target_name!r} not found in ontology")
    elif (subj, RDF.type, OWL.Class) not in g:
        raise ValueError(f"class {target_name!r} not found in ontology")
    g.add((subj, predicate, value))
    return g.serialize(format="turtle")


def _handle_add_label(ontology_ttl, namespace, fix):
    new_value = fix.get("value", "").strip()
    if not new_value:
        raise ValueError("fix.value is required for add_label")
    new_ttl = _add_triple(ontology_ttl, namespace, fix["target"], RDFS.label, Literal(new_value))
    return new_ttl, {"added": "rdfs:label", "value": new_value, "target": fix["target"]}


def _handle_add_description(ontology_ttl, namespace, fix):
    new_value = fix.get("value", "").strip()
    if not new_value:
        raise ValueError("fix.value is required for add_description")
    new_ttl = _add_triple(ontology_ttl, namespace, fix["target"], SKOS.definition, Literal(new_value))
    return new_ttl, {"added": "skos:definition", "value": new_value, "target": fix["target"]}


def _handle_add_class(ontology_ttl, namespace, fix):
    """Delegates to ontology_editor.add_class — same path as the inline
    editor + the Builder. Reused unchanged."""
    from pipeline.ontology_editor import add_class
    return add_class(
        ontology_ttl, namespace,
        local_name=fix.get("name", ""),
        label=fix.get("label"),
        description=fix.get("description"),
    )


def _handle_add_datatype_property(ontology_ttl, namespace, fix):
    from pipeline.ontology_editor import add_datatype_property
    return add_datatype_property(
        ontology_ttl, namespace,
        local_name=fix.get("name", ""),
        domain_class=fix.get("domain", ""),
        xsd_range=fix.get("range", "string"),
        label=fix.get("label"),
    )


def _handle_add_object_property(ontology_ttl, namespace, fix):
    from pipeline.ontology_editor import add_object_property
    return add_object_property(
        ontology_ttl, namespace,
        local_name=fix.get("name", ""),
        domain_class=fix.get("domain", ""),
        range_class=fix.get("range", ""),
        functional=bool(fix.get("functional", True)),
        label=fix.get("label"),
    )


def _handle_convert_to_object(ontology_ttl, namespace, fix):
    """Convert a datatype property into an object property when the
    linter detected a hidden FK pattern (column ends in 'Id', class
    with that base name exists). Removes the old datatype property
    triples, adds a new object property linking the existing domain
    class to the matching range class."""
    from pipeline.ontology_editor import add_object_property
    target_kind, old_name = _resolve_target(fix["target"])
    if target_kind != "property":
        raise ValueError("convert_to_object requires target='property:<name>'")
    new_name = fix.get("new_property", "").strip()
    range_class = fix.get("range_class", "").strip()
    if not new_name or not range_class:
        raise ValueError("fix.new_property and fix.range_class are required")

    g = Graph()
    g.parse(data=ontology_ttl, format="turtle")
    old_uri = URIRef(namespace + old_name)
    if (old_uri, RDF.type, OWL.DatatypeProperty) not in g:
        raise ValueError(f"datatype property {old_name!r} not found")

    # Capture the old domain — it stays as the new object property's
    # domain. Multiple domains are unusual; we reuse the first.
    old_domains = list(g.objects(old_uri, RDFS.domain))
    if not old_domains:
        raise ValueError(
            f"property {old_name!r} has no domain; convert_to_object needs a "
            "domain to link from. Add the domain first via Edit Ontology."
        )
    domain_local = _local_uri(old_domains[0])

    # Remove every triple referring to the old property.
    for triple in list(g.triples((old_uri, None, None))):
        g.remove(triple)
    for triple in list(g.triples((None, None, old_uri))):
        g.remove(triple)

    intermediate = g.serialize(format="turtle")
    final, _ = add_object_property(
        intermediate, namespace,
        local_name=new_name,
        domain_class=domain_local,
        range_class=range_class,
        functional=True,
        label=new_name,
    )
    return final, {
        "removed": f"datatype property {old_name}",
        "added": f"object property {new_name} ({domain_local} → {range_class})",
    }


def _handle_noop(*_args, **_kwargs):
    raise ValueError(
        "This finding has no automatic fix — it requires an operator decision. "
        "Open the Use Cases tab → Edit Ontology to make the change manually."
    )


def _local_uri(uri) -> str:
    s = str(uri)
    for sep in ("#", "/"):
        if sep in s:
            return s.rsplit(sep, 1)[1]
    return s


_HANDLERS = {
    "add_label": _handle_add_label,
    "add_description": _handle_add_description,
    "add_class": _handle_add_class,
    "add_datatype_property": _handle_add_datatype_property,
    "add_object_property": _handle_add_object_property,
    "convert_to_object": _handle_convert_to_object,
    "noop": _handle_noop,
}
