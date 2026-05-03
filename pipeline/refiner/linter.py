"""Rule-based ontology linter.

Walks the bundle's ontology TTL with rdflib + reads the manifest, then
runs a series of checks. Each check yields zero or more finding dicts.

Designed to be cheap (sub-100ms on the shipped bundles), deterministic,
and side-effect-free. The LLM coach in llm_coach.py runs the more
expensive structural-suggestion checks.

To add a new rule: write a function `_check_<name>(ctx) -> list[dict]`
and register it in `_RULES` below. Each finding must include an `id`
unique within one lint() call (the applicator + UI rely on stable IDs
to track which findings the user has dismissed/applied).
"""
from __future__ import annotations
import re
from typing import Iterable

from rdflib import Graph, OWL, RDF, RDFS, URIRef
from rdflib.namespace import SKOS

from pipeline.use_case import UseCase


# ── Linting context ────────────────────────────────────────────────────────

class _LintContext:
    """Pre-parsed bundle state shared across rules so we don't re-parse
    the TTL inside each check.

    Two construction paths:
      - from_use_case(uc) — reads ontology.ttl from disk
      - from_text(ttl, prefix, namespace) — for in-memory state (Builder
        preview step, where the bundle hasn't been written yet)
    """

    def __init__(self, ttl_text: str, prefix: str, namespace: str, manifest=None):
        self.manifest = manifest      # may be None when called from text
        self.prefix = prefix
        self.ns = namespace
        self.g = Graph()
        if ttl_text and ttl_text.strip():
            try:
                self.g.parse(data=ttl_text, format="turtle")
            except Exception:
                # Bad TTL is the curation tab's job to surface; the
                # linter just refuses to run rather than throw.
                pass

        # Index local-name → URIRef for O(1) lookups in rules.
        self.classes_uris: dict[str, URIRef] = {
            _local(c): c for c in self.g.subjects(RDF.type, OWL.Class) if isinstance(c, URIRef)
        }
        self.dt_props_uris: dict[str, URIRef] = {
            _local(p): p for p in self.g.subjects(RDF.type, OWL.DatatypeProperty) if isinstance(p, URIRef)
        }
        self.obj_props_uris: dict[str, URIRef] = {
            _local(p): p for p in self.g.subjects(RDF.type, OWL.ObjectProperty) if isinstance(p, URIRef)
        }

    @classmethod
    def from_use_case(cls, use_case: UseCase) -> "_LintContext":
        """Convenience constructor for the on-disk path. Reads the
        bundle's ontology.ttl + manifest fields."""
        ttl = use_case.ontology_path.read_text(encoding="utf-8") if use_case.ontology_path.exists() else ""
        ctx = cls(ttl, use_case.manifest.prefix, use_case.manifest.namespace, manifest=use_case.manifest)
        ctx.use_case = use_case
        return ctx

    # Convenience helpers shared across rules.

    def labels_for(self, uri: URIRef) -> list[str]:
        return [str(o) for o in self.g.objects(uri, RDFS.label)]

    def has_label(self, uri: URIRef) -> bool:
        return bool(next(self.g.objects(uri, RDFS.label), None))

    def has_definition(self, uri: URIRef) -> bool:
        return bool(next(self.g.objects(uri, SKOS.definition), None))

    def domains_of(self, prop_uri: URIRef) -> list[URIRef]:
        return [d for d in self.g.objects(prop_uri, RDFS.domain) if isinstance(d, URIRef)]

    def ranges_of(self, prop_uri: URIRef) -> list[URIRef]:
        return [r for r in self.g.objects(prop_uri, RDFS.range) if isinstance(r, URIRef)]


def _local(uri: URIRef) -> str:
    s = str(uri)
    for sep in ("#", "/"):
        if sep in s:
            return s.rsplit(sep, 1)[1]
    return s


# ── Rules ──────────────────────────────────────────────────────────────────
# Each rule returns a list of findings. Finding `id`s should be stable
# across lint() runs against the same bundle so the UI can track
# user-dismissed findings between sessions if we add that later.

def _check_class_has_label(ctx: _LintContext) -> list[dict]:
    findings = []
    for local, uri in ctx.classes_uris.items():
        if not ctx.has_label(uri):
            findings.append({
                "id": f"class-no-label-{local}",
                "source": "lint",
                "severity": "info",
                "category": "labels",
                "title": f"Class {local} has no rdfs:label",
                "description": "A human-readable label makes the class clearer in the visualisation, agent prompts, and Cypher autocomplete.",
                "fix": {
                    "kind": "add_label",
                    "target": f"class:{local}",
                    "value": _humanise(local),
                    "preview": f'rdfs:label "{_humanise(local)}"',
                },
            })
    return findings


def _check_class_has_definition(ctx: _LintContext) -> list[dict]:
    findings = []
    for local, uri in ctx.classes_uris.items():
        if not ctx.has_definition(uri):
            findings.append({
                "id": f"class-no-definition-{local}",
                "source": "lint",
                "severity": "info",
                "category": "labels",
                "title": f"Class {local} has no skos:definition",
                "description": "A one-sentence definition helps operators (and the LLM) know what each class represents. Especially useful when class names are ambiguous.",
                "fix": {
                    "kind": "add_description",
                    "target": f"class:{local}",
                    "value": f"A {_humanise(local).lower()} entity.",
                    "preview": f'skos:definition "A {_humanise(local).lower()} entity."',
                },
            })
    return findings


def _check_property_has_label(ctx: _LintContext) -> list[dict]:
    findings = []
    for kind_label, uris in (("datatype property", ctx.dt_props_uris), ("object property", ctx.obj_props_uris)):
        for local, uri in uris.items():
            if not ctx.has_label(uri):
                findings.append({
                    "id": f"prop-no-label-{local}",
                    "source": "lint",
                    "severity": "info",
                    "category": "labels",
                    "title": f"{kind_label.capitalize()} {local} has no rdfs:label",
                    "description": "Property labels show up in property-detail panels and the Cypher autocomplete tooltips.",
                    "fix": {
                        "kind": "add_label",
                        "target": f"property:{local}",
                        "value": _humanise(local),
                        "preview": f'rdfs:label "{_humanise(local)}"',
                    },
                })
    return findings


def _check_orphan_class(ctx: _LintContext) -> list[dict]:
    """A class with NO datatype properties AND NO object property
    domain/range references is a curiosity — almost certainly a
    misconfiguration."""
    findings = []
    for local, uri in ctx.classes_uris.items():
        # Does any datatype property point at this class via rdfs:domain?
        is_domain = any(uri in ctx.domains_of(p) for p in ctx.dt_props_uris.values())
        # Object property pointing IN or OUT?
        is_endpoint = any(
            uri in ctx.domains_of(p) or uri in ctx.ranges_of(p)
            for p in ctx.obj_props_uris.values()
        )
        if not is_domain and not is_endpoint:
            findings.append({
                "id": f"orphan-class-{local}",
                "source": "lint",
                "severity": "warn",
                "category": "isolation",
                "title": f"Class {local} has no properties or relationships",
                "description": "An orphan class can't carry any data and can't participate in queries. Either add at least one datatype property, link it via an object property, or remove it from the ontology.",
                "fix": {
                    "kind": "noop",   # No safe automatic fix; needs operator decision.
                    "target": f"class:{local}",
                    "preview": "(no automatic fix — open Edit Ontology to add a property or remove the class)",
                },
            })
    return findings


def _check_isolated_class(ctx: _LintContext) -> list[dict]:
    """A class with datatype properties but no object-property links —
    won't appear connected in the graph viz. Less serious than orphan
    (the data still loads), but worth flagging."""
    findings = []
    for local, uri in ctx.classes_uris.items():
        is_endpoint = any(
            uri in ctx.domains_of(p) or uri in ctx.ranges_of(p)
            for p in ctx.obj_props_uris.values()
        )
        is_domain = any(uri in ctx.domains_of(p) for p in ctx.dt_props_uris.values())
        if is_domain and not is_endpoint:
            findings.append({
                "id": f"isolated-class-{local}",
                "source": "lint",
                "severity": "info",
                "category": "isolation",
                "title": f"Class {local} has no relationships",
                "description": "Nodes will load but won't be connected to anything else in the graph. If this is intentional (a leaf entity), dismiss this finding. Otherwise add an object property linking it to another class.",
                "fix": {
                    "kind": "noop",
                    "target": f"class:{local}",
                    "preview": "(no automatic fix — use Edit Ontology to add an object property)",
                },
            })
    return findings


_ID_SUFFIX_RE = re.compile(r"(?<!^)Id$|^id$")


def _check_potential_object_property(ctx: _LintContext) -> list[dict]:
    """A datatype property whose name ends in 'Id' (camelCase) might be
    a hidden foreign key. Flag it so the operator can decide whether
    to convert it to an object property."""
    findings = []
    for local, uri in ctx.dt_props_uris.items():
        if not _ID_SUFFIX_RE.search(local):
            continue
        # Only suggest if a class with the matching base name exists.
        # e.g. customerId → does a Customer class exist?
        base = local[:-2] if local.endswith("Id") else local
        candidate_class = base[:1].upper() + base[1:]
        if candidate_class not in ctx.classes_uris:
            continue
        # Skip if this property is the PK of its own class (then it's
        # the natural key, not a foreign key).
        domains = ctx.domains_of(uri)
        if any(_local(d) == candidate_class for d in domains):
            continue
        findings.append({
            "id": f"hidden-fk-{local}",
            "source": "lint",
            "severity": "info",
            "category": "structure",
            "title": f"Property {local} looks like a foreign key to {candidate_class}",
            "description": f"Datatype property {local} ends in 'Id' AND a {candidate_class} class exists in this ontology. Consider modelling it as an object property `{base}` linking to {candidate_class} so graph queries can traverse the relationship directly.",
            "fix": {
                "kind": "convert_to_object",
                "target": f"property:{local}",
                "new_property": base,
                "range_class": candidate_class,
                "preview": f"new owl:ObjectProperty `{base}` (domain: <existing>, range: {candidate_class})",
            },
        })
    return findings


_CAMELCASE_RE = re.compile(r"^[a-z][a-zA-Z0-9]*$")
_PASCALCASE_RE = re.compile(r"^[A-Z][a-zA-Z0-9]*$")


def _check_naming_conventions(ctx: _LintContext) -> list[dict]:
    """Warn about names that don't follow the platform's conventions
    (PascalCase classes, camelCase properties). Affects how nicely the
    NL→Cypher prompt + autocomplete work."""
    findings = []
    for local in ctx.classes_uris:
        if not _PASCALCASE_RE.match(local):
            findings.append({
                "id": f"class-naming-{local}",
                "source": "lint",
                "severity": "info",
                "category": "naming",
                "title": f"Class {local} doesn't follow PascalCase",
                "description": "PascalCase (e.g. WorkOrder, Customer) is the convention across all bundles. Mixing styles makes the Cypher autocomplete and agent prompts inconsistent.",
                "fix": {
                    "kind": "noop",
                    "target": f"class:{local}",
                    "preview": "(rename via Edit Ontology — manual to avoid breaking existing data)",
                },
            })
    for local in (*ctx.dt_props_uris, *ctx.obj_props_uris):
        if not _CAMELCASE_RE.match(local):
            findings.append({
                "id": f"prop-naming-{local}",
                "source": "lint",
                "severity": "info",
                "category": "naming",
                "title": f"Property {local} doesn't follow camelCase",
                "description": "camelCase (e.g. workOrderId, createdAt) is the convention across all bundles.",
                "fix": {
                    "kind": "noop",
                    "target": f"property:{local}",
                    "preview": "(rename via Edit Ontology — manual to avoid breaking existing data)",
                },
            })
    return findings


def _check_property_has_domain(ctx: _LintContext) -> list[dict]:
    """A property without an rdfs:domain is unscoped — it can attach to
    any node, which makes the schema ambiguous."""
    findings = []
    for kind_label, uris in (("datatype property", ctx.dt_props_uris), ("object property", ctx.obj_props_uris)):
        for local, uri in uris.items():
            if not ctx.domains_of(uri):
                findings.append({
                    "id": f"prop-no-domain-{local}",
                    "source": "lint",
                    "severity": "warn",
                    "category": "constraints",
                    "title": f"{kind_label.capitalize()} {local} has no rdfs:domain",
                    "description": "Without a domain, the property can attach to any node. SHACL validation can't enforce its presence and the autocomplete can't suggest it scoped to a class.",
                    "fix": {
                        "kind": "noop",
                        "target": f"property:{local}",
                        "preview": "(no automatic fix — domain is operator's choice)",
                    },
                })
    return findings


def _check_object_property_has_range(ctx: _LintContext) -> list[dict]:
    """An object property without rdfs:range can target any node —
    same problem as missing domain but on the other side of the arrow."""
    findings = []
    for local, uri in ctx.obj_props_uris.items():
        if not ctx.ranges_of(uri):
            findings.append({
                "id": f"objprop-no-range-{local}",
                "source": "lint",
                "severity": "warn",
                "category": "constraints",
                "title": f"Object property {local} has no rdfs:range",
                "description": "Without a range, the property can target any class. SHACL can't enforce the relationship's destination.",
                "fix": {
                    "kind": "noop",
                    "target": f"property:{local}",
                    "preview": "(no automatic fix — range is operator's choice)",
                },
            })
    return findings


# ── Helpers ────────────────────────────────────────────────────────────────

def _humanise(camel_or_pascal: str) -> str:
    """workOrderId → 'Work Order Id'; WorkOrder → 'Work Order'."""
    s = re.sub(r"([A-Z])", r" \1", camel_or_pascal).strip()
    return s[:1].upper() + s[1:]


# ── Public entry point ─────────────────────────────────────────────────────

# Order matters for UI grouping: high-impact first, naming/labels last.
_RULES = [
    _check_orphan_class,
    _check_property_has_domain,
    _check_object_property_has_range,
    _check_potential_object_property,
    _check_isolated_class,
    _check_class_has_label,
    _check_class_has_definition,
    _check_property_has_label,
    _check_naming_conventions,
]


def lint(use_case: UseCase) -> dict:
    """Run every linter rule against the bundle's ontology + manifest.

    Returns a dict with `findings: list[dict]` plus per-category counts
    so the UI can render summary chips before the user expands the
    detail. Findings ordered by severity (error → warn → info), then
    by category, then alphabetically by id.
    """
    return _run_rules(_LintContext.from_use_case(use_case))


def lint_text(ontology_ttl: str, prefix: str, namespace: str) -> dict:
    """Same as lint() but operates on in-memory TTL text. Used by the
    Builder's Preview step where the bundle isn't on disk yet — gives
    the operator lint findings BEFORE clicking Create so they can
    iterate on the schema dict + regenerate without touching disk."""
    return _run_rules(_LintContext(ontology_ttl, prefix, namespace))


def _run_rules(ctx: _LintContext) -> dict:
    all_findings: list[dict] = []
    for rule in _RULES:
        try:
            all_findings.extend(rule(ctx))
        except Exception as exc:
            all_findings.append({
                "id": f"rule-error-{rule.__name__}",
                "source": "lint",
                "severity": "error",
                "category": "internal",
                "title": f"Linter rule {rule.__name__} crashed",
                "description": f"{exc}. This is a bug in the linter, not your ontology.",
                "fix": {"kind": "noop", "target": "", "preview": ""},
            })

    severity_rank = {"error": 0, "warn": 1, "info": 2}
    all_findings.sort(key=lambda f: (severity_rank.get(f["severity"], 9), f["category"], f["id"]))

    counts = {"error": 0, "warn": 0, "info": 0}
    by_category: dict[str, int] = {}
    for f in all_findings:
        counts[f["severity"]] = counts.get(f["severity"], 0) + 1
        by_category[f["category"]] = by_category.get(f["category"], 0) + 1

    return {
        "findings": all_findings,
        "counts": counts,
        "by_category": by_category,
        "total": len(all_findings),
    }
