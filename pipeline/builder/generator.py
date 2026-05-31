"""Generate a complete bundle (manifest.yaml + ontology.ttl + data.ttl)
from an inspected schema dict.

The schema dict shape is the contract between inspectors (csv,
postgres, future ones) and the generator. See docs/ontology-builder.md
for the full reference.

Validation contract: the produced manifest is round-tripped through the
`Manifest` Pydantic model BEFORE being returned. The ontology TTL is
re-parsed by rdflib so we never hand back something that wouldn't
load. Failures raise ValueError with a precise reason — the route
layer surfaces those to the wizard's preview step.

Reuses the building-block ops from pipeline.ontology_editor:
  add_class / add_datatype_property / add_object_property
…each of which is itself a thin rdflib mutation. The generator just
sequences the right calls in the right order (classes first, then
datatype properties, then object properties — so the FK target
classes already exist when the relationships go in).
"""
from __future__ import annotations
import re
from typing import Any

import yaml
from rdflib import Graph, Namespace

from pipeline.ontology_editor import (
    add_class, add_datatype_property, add_object_property,
)
from pipeline.use_case import Manifest


# ── Naming + safety ─────────────────────────────────────────────────────────

_NAME_RE = re.compile(r"^[A-Za-z][A-Za-z0-9_]{0,63}$")
_SLUG_RE = re.compile(r"^[a-z0-9][a-z0-9_-]{0,63}$")
_PREFIX_RE = re.compile(r"^[a-z][a-z0-9_-]{0,15}$")
# xsd ranges the inline editor accepts (kept in sync with
# pipeline/ontology_editor._XSD_RANGES).
_VALID_XSD = {"string", "integer", "decimal", "boolean", "date", "dateTime"}


def _validate_class_name(name: str) -> str:
    if not _NAME_RE.match(name):
        raise ValueError(
            f"Invalid class name {name!r}: must match {_NAME_RE.pattern} "
            "(starts with a letter, then letters/digits/underscores, ≤64 chars)."
        )
    return name


def _validate_property_name(name: str) -> str:
    if not _NAME_RE.match(name):
        raise ValueError(
            f"Invalid property name {name!r}: must match {_NAME_RE.pattern}."
        )
    return name


def _validate_xsd_range(xsd_type: str) -> str:
    if xsd_type not in _VALID_XSD:
        raise ValueError(
            f"xsd type {xsd_type!r} not supported. Choose one of: "
            f"{sorted(_VALID_XSD)}"
        )
    return xsd_type


# ── Helpers for SQL / TTL escaping ───────────────────────────────────────────

def _escape_ttl_literal(value: Any, xsd_type: str = "string") -> str:
    """Render a Python value as a TTL literal. Quotes are escaped for
    string literals; numbers/booleans/dates use their typed forms."""
    if value is None:
        return None  # caller should skip this triple
    if xsd_type == "boolean":
        return "true" if str(value).lower() in ("true", "t", "1", "yes") else "false"
    if xsd_type in ("integer", "decimal"):
        # Don't quote — let the parser validate.
        return str(value)
    if xsd_type in ("date", "dateTime"):
        # Quote with explicit type so rdflib parses it right.
        s = str(value).replace('"', r'\"')
        return f'"{s}"^^xsd:{xsd_type}'
    # String fallback — escape any embedded double-quotes.
    s = str(value).replace("\\", "\\\\").replace('"', r'\"').replace("\n", r"\n")
    return f'"{s}"'


def _quote_sql_ident(ident: str) -> str:
    """Postgres identifier quoting — wraps in double quotes and escapes
    embedded ones. Use for column/table names that came from user input."""
    return '"' + ident.replace('"', '""') + '"'


# ── Bundle metadata helpers ─────────────────────────────────────────────────

def _suggest_namespace(prefix: str) -> str:
    return f"http://example.org/{prefix}#"


def _validate_bundle_meta(meta: dict) -> dict:
    """Fill defaults + sanity-check the bundle metadata block from the wizard."""
    slug = meta.get("slug", "").strip()
    prefix = meta.get("prefix", "").strip()
    namespace = meta.get("namespace", "").strip()
    if not _SLUG_RE.match(slug):
        raise ValueError(
            f"Invalid bundle slug {slug!r}: must match {_SLUG_RE.pattern} "
            "(lowercase, 1–64 chars, starts with letter/digit, only -/_ as separators)."
        )
    if not _PREFIX_RE.match(prefix):
        raise ValueError(
            f"Invalid prefix {prefix!r}: must match {_PREFIX_RE.pattern} "
            "(lowercase, ≤16 chars, starts with letter)."
        )
    if not namespace:
        namespace = _suggest_namespace(prefix)
    if not namespace.endswith(("#", "/")):
        raise ValueError(
            f"Namespace {namespace!r} must end with '#' or '/' so the n10s "
            "SHORTEN-mode prefix system can append local names."
        )
    return {
        "slug": slug,
        "name": (meta.get("name") or "").strip() or slug.replace("-", " ").title(),
        "description": (meta.get("description") or "").strip(),
        "prefix": prefix,
        "namespace": namespace,
    }


# ── Schema dict validation ──────────────────────────────────────────────────

def _validate_schema(schema: dict) -> dict:
    """Sanity-check the inspector's output before generating anything.
    We're paranoid about this — a bad schema dict means a bad bundle, and
    debugging "why doesn't this ontology parse" is way harder than
    surfacing the root cause here."""
    if not isinstance(schema, dict):
        raise ValueError("schema must be a dict")
    if schema.get("source_kind") not in ("postgres", "csv", "description"):
        raise ValueError(
            f"source_kind must be 'postgres', 'csv', or 'description', "
            f"got {schema.get('source_kind')!r}"
        )
    tables = schema.get("tables") or []
    if not tables:
        raise ValueError("schema has no tables — inspector returned an empty result")
    seen = set()
    for t in tables:
        cls = _validate_class_name(t.get("class_name", ""))
        if cls in seen:
            raise ValueError(f"Duplicate class name {cls!r} — rename in the wizard.")
        seen.add(cls)
        for col in t.get("columns") or []:
            _validate_property_name(col.get("name", ""))
            _validate_xsd_range(col.get("xsd_type", "string"))
    return schema


# ── Main generator ──────────────────────────────────────────────────────────

def generate(schema: dict, bundle_meta: dict) -> dict[str, Any]:
    """Produce ontology.ttl + manifest.yaml + data.ttl from an inspected
    schema dict and bundle metadata.

    Returns:
      {
        "ontology_ttl": str,
        "manifest_yaml": str,
        "data_ttl": str,
        "summary": {"classes": N, "datatype_properties": N,
                    "object_properties": N, "data_triples": N},
      }
    """
    schema = _validate_schema(schema)
    meta = _validate_bundle_meta(bundle_meta)

    # 1. Build the ontology TTL incrementally via the existing editor
    #    helpers. Order: classes → datatype properties → object
    #    properties (so FK targets exist when relationships are added).
    onto = _seed_ontology_ttl(meta)

    # Pass 1: classes — honour user-set label + description if present, else
    # default to the auto-generated values. Empty strings count as "use default"
    # so the wizard can wipe a field to reset it.
    for table in schema["tables"]:
        cls_label = (table.get("class_label") or "").strip() or _humanise(table["class_name"])
        cls_desc = (table.get("class_description") or "").strip() or \
            f"Generated from {schema['source_kind']} table {table['name']!r}"
        onto, _ = add_class(
            onto, meta["namespace"], table["class_name"],
            label=cls_label,
            description=cls_desc,
        )

    # Pass 2: datatype properties — one per column. Honour per-column
    # label override; description override is stored in the schema dict
    # but not yet wired into add_datatype_property (would need an
    # ontology_editor extension; deferred for v1).
    for table in schema["tables"]:
        for col in table.get("columns", []):
            col_label = (col.get("label") or "").strip() or _humanise(col["name"])
            try:
                onto, _ = add_datatype_property(
                    onto, meta["namespace"],
                    local_name=col["name"],
                    domain_class=table["class_name"],
                    xsd_range=col["xsd_type"],
                    label=col_label,
                )
            except ValueError as exc:
                # Ignore duplicate-property errors (same col name on two
                # tables). The first wins; surface this in the summary.
                if "already exists" not in str(exc):
                    raise

    # Pass 3a: object properties from Postgres foreign keys.
    obj_count = 0
    if schema.get("source_kind") == "postgres":
        # Index tables by their original SQL name so FKs can find the class.
        by_sql_name = {t["name"]: t["class_name"] for t in schema["tables"]}
        for table in schema["tables"]:
            for fk in table.get("foreign_keys", []) or []:
                ref_class = by_sql_name.get(fk.get("ref_table"))
                if not ref_class:
                    continue   # FK target not in inspected schema; skip
                rel_name = _singular(fk["ref_table"])
                # Sanitize: "user-data" → "userdata". camelCase already.
                rel_name = re.sub(r"[^A-Za-z0-9_]", "", rel_name)
                if not rel_name or not _NAME_RE.match(rel_name):
                    continue
                try:
                    onto, _ = add_object_property(
                        onto, meta["namespace"],
                        local_name=rel_name,
                        domain_class=table["class_name"],
                        range_class=ref_class,
                        functional=True,   # FKs typically point to one row
                    )
                    obj_count += 1
                except ValueError as exc:
                    if "already exists" not in str(exc):
                        raise

    # Pass 3b: explicit user-added relationships (works for any source).
    # CSV source uses this exclusively (no FK detection); Postgres source
    # can use it to add relationships the inspector missed.
    for table in schema["tables"]:
        for rel in table.get("relationships", []) or []:
            rel_name = (rel.get("name") or "").strip()
            range_class = (rel.get("range_class") or "").strip()
            if not rel_name or not range_class:
                continue
            if not _NAME_RE.match(rel_name):
                raise ValueError(
                    f"Relationship name {rel_name!r} on {table['class_name']} "
                    f"must match {_NAME_RE.pattern}."
                )
            try:
                onto, _ = add_object_property(
                    onto, meta["namespace"],
                    local_name=rel_name,
                    domain_class=table["class_name"],
                    range_class=range_class,
                    functional=bool(rel.get("functional", False)),
                    label=(rel.get("label") or "").strip() or _humanise(rel_name),
                )
                obj_count += 1
            except ValueError as exc:
                if "already exists" not in str(exc):
                    raise

    # 2. Build the manifest. Postgres source pre-populates datasources +
    #    pull adapters so the bundle is hydration-ready immediately.
    manifest_dict = _build_manifest(schema, meta)

    # 3. Validate the manifest against the production Pydantic model BEFORE
    #    returning anything. Ensures any bundle the wizard emits will load.
    Manifest(**manifest_dict)

    # 4. Build data.ttl. Postgres → empty (pulls handle loading);
    #    CSV → seed with one node per row; description → synthetic seed data
    #    generated from the ontology (no datasource to pull from). The latter
    #    needs the just-built ontology TTL, so pass it through.
    data_ttl, data_triples = _build_data_ttl(schema, meta, onto)

    # 5. Final sanity check — re-parse the ontology to confirm rdflib
    #    still accepts it after all our mutations.
    g = Graph()
    g.parse(data=onto, format="turtle")

    summary = _summarize(schema, obj_count, data_triples)
    manifest_yaml = yaml.safe_dump(manifest_dict, sort_keys=False, allow_unicode=True)
    return {
        "ontology_ttl": onto,
        "manifest_yaml": manifest_yaml,
        "data_ttl": data_ttl,
        "summary": summary,
    }


def _seed_ontology_ttl(meta: dict) -> str:
    """Initial TTL with prefix declarations + ontology header. The editor
    helpers will append owl:Class / owl:DatatypeProperty / owl:ObjectProperty
    triples on top."""
    return (
        f"@prefix {meta['prefix']}: <{meta['namespace']}> .\n"
        f"@prefix owl:  <http://www.w3.org/2002/07/owl#> .\n"
        f"@prefix rdf:  <http://www.w3.org/1999/02/22-rdf-syntax-ns#> .\n"
        f"@prefix rdfs: <http://www.w3.org/2000/01/rdf-schema#> .\n"
        f"@prefix xsd:  <http://www.w3.org/2001/XMLSchema#> .\n"
        f"@prefix skos: <http://www.w3.org/2004/02/skos/core#> .\n"
        f"@prefix dcterms: <http://purl.org/dc/terms/> .\n"
        f"\n"
        f"<{meta['namespace'].rstrip('#/')}>\n"
        f"    a owl:Ontology ;\n"
        f"    dcterms:title \"{_escape_ttl_string(meta['name'])}\" ;\n"
        f"    dcterms:description \"Generated by KF Ontology Builder.\" ;\n"
        f"    owl:versionInfo \"0.1.0\" .\n"
        f"\n"
    )


def _escape_ttl_string(s: str) -> str:
    return s.replace("\\", "\\\\").replace('"', r'\"').replace("\n", r"\n")


def _build_manifest(schema: dict, meta: dict) -> dict:
    """Assemble the manifest dict. Postgres adds datasources + pull
    adapters; CSV stays minimal. Both sources get auto-generated
    examples + nl_rules so the Query Console has clickable starter
    queries on day one."""
    in_scope = [t["class_name"] for t in schema["tables"]]
    manifest: dict = {
        "slug": meta["slug"],
        "name": meta["name"],
        "description": meta["description"] or f"Generated from {schema['source_kind']}.",
        "prefix": meta["prefix"],
        "namespace": meta["namespace"],
        "in_scope_classes": in_scope,
    }
    if schema["source_kind"] == "postgres":
        env_var = (schema.get("source_metadata") or {}).get("dsn_env")
        if env_var:
            ds_id = "source_db"
            manifest["datasources"] = [
                {"id": ds_id, "kind": "postgres", "dsn_env": env_var},
            ]
            adapters = []
            for i, table in enumerate(schema["tables"], start=1):
                adapters.append(_build_pull_adapter(ds_id, table, i))
            manifest["stage4_adapters"] = adapters

    # Carry per-column enum hints into the manifest so data_generator picks
    # class-appropriate values instead of the generic _STATUSES list. Hints
    # come from the description-source nl_inspector (which extracts them
    # from prose like "status (approved/rejected/pending)") and from any
    # future inspector that wants to seed enum vocabularies. Absence keeps
    # existing default-randomised behaviour intact.
    enum_hints_by_class: dict[str, dict[str, list[str]]] = {}
    for table in schema["tables"]:
        cls = table["class_name"]
        for col in table.get("columns", []) or []:
            hints = col.get("enum_hints") or []
            if hints:
                enum_hints_by_class.setdefault(cls, {})[col["name"]] = list(hints)
    if enum_hints_by_class:
        manifest["sample_enum_values_hints"] = enum_hints_by_class

    # Starter examples — give every new bundle a Query Console that's
    # actually useful out of the box. Capped at MAX_EXAMPLES to keep the
    # chip strip manageable; per-class breadth wins over per-class depth.
    examples, nl_rules = _build_examples(schema, meta)
    if examples:
        manifest["examples"] = examples
    if nl_rules:
        manifest["nl_rules"] = nl_rules
    return manifest


# Cap on auto-generated examples so the Query Console chip strip stays
# scrollable. Distributes ~2-3 examples per class up to this total.
MAX_EXAMPLES = 12


def _pk_of(table: dict) -> str | None:
    """The bundle's chosen primary key for a class, falling back to the
    first column when the inspector didn't pin one. Returns None only for
    pathological classes with zero columns (which the schema validator
    would already reject upstream, but we don't want to crash here)."""
    pk = table.get("primary_key")
    if pk:
        return pk
    cols = table.get("columns") or []
    return cols[0]["name"] if cols else None


def _relationships_from_schema(schema: dict) -> list[tuple[str, str, str]]:
    """Flatten the schema's relationships into a list of
    (domain_class, rel_local_name, range_class) tuples.

    Pulls from both `relationships` (explicit, all source kinds) and
    `foreign_keys` (Postgres). The traversal/multi-hop/count templates
    don't care which side of that fence the link came from."""
    rels: list[tuple[str, str, str]] = []
    by_sql_name = {t["name"]: t["class_name"] for t in schema.get("tables", [])}
    for table in schema.get("tables", []):
        domain = table["class_name"]
        for rel in table.get("relationships", []) or []:
            name = (rel.get("name") or "").strip()
            range_class = (rel.get("range_class") or "").strip()
            if name and range_class and _NAME_RE.match(name):
                rels.append((domain, name, range_class))
        if schema.get("source_kind") == "postgres":
            for fk in table.get("foreign_keys", []) or []:
                ref_class = by_sql_name.get(fk.get("ref_table"))
                if not ref_class:
                    continue
                rel_name = re.sub(r"[^A-Za-z0-9_]", "", _singular(fk["ref_table"]))
                if not rel_name or not _NAME_RE.match(rel_name):
                    continue
                rels.append((domain, rel_name, ref_class))
    return rels


def _find_two_hop_chains(
    rels: list[tuple[str, str, str]],
) -> list[tuple[str, str, str, str, str]]:
    """Find every directed 2-hop chain A-[r1]->B-[r2]->C in the
    relationship list. Returns (A, r1, B, r2, C) tuples. Used by the
    multi-hop example template — operators love these because they
    answer 'what does X connect to via Y?' which is hard to discover
    from the chip-strip otherwise.

    BFS-equivalent for depth-2: we just enumerate adjacent pairs. Skips
    pure self-loops (A→A→A) to keep examples diverse, but allows
    A→B→A because that's often a meaningful 'round-trip' pattern."""
    by_domain: dict[str, list[tuple[str, str]]] = {}
    for d, r, rng in rels:
        by_domain.setdefault(d, []).append((r, rng))
    chains: list[tuple[str, str, str, str, str]] = []
    for a, r1, b in rels:
        for r2, c in by_domain.get(b, []):
            if a == b == c:
                continue
            chains.append((a, r1, b, r2, c))
    return chains


def _build_examples(schema: dict, meta: dict) -> tuple[list[dict], list[dict]]:
    """Generate up to MAX_EXAMPLES starter Cypher examples + matching NL rules.

    Shape (matches the plan in docs/ontology-builder.md):
      3 simple examples on the "main" class (most columns):
        1. "Show all <plural>"     → MATCH (n:<Cls>) RETURN n LIMIT 25
        2. "Count <plural>"        → MATCH (n:<Cls>) RETURN count(n) AS total
        3. "Top 10 <plural> by <pk>" (no nl_rule — too narrow to match)
      Up to (MAX_EXAMPLES - 3) complex examples, in priority order:
        a. Relationship traversal per object property
        b. Counts per related class (top-N aggregation)
        c. Multi-hop A→B→C chains discovered by BFS over rels
        d. Enum filter (one per property with enum_hints attached)

    Simple ones get matching nl_rules; complex ones are shown as Cypher
    chips only (their query shape is too varied for useful regex hits).
    """
    prefix = meta["prefix"]
    examples: list[dict] = []
    nl_rules: list[dict] = []

    def _add(label: str, cypher: str, nl_pattern: str | None) -> bool:
        """Append example + optional nl_rule. Returns False when the
        cap is full so callers can stop building more candidates."""
        if len(examples) >= MAX_EXAMPLES:
            return False
        idx = len(examples)
        examples.append({"label": label, "cypher": cypher})
        if nl_pattern:
            nl_rules.append({"pattern": nl_pattern, "example_index": idx})
        return True

    tables = schema.get("tables") or []
    if not tables:
        return examples, nl_rules

    # ── 3 simple examples on the "main" class ──────────────────────────────
    # "Main" = most columns; ties broken by table order so callers get a
    # deterministic shape across runs of the same schema.
    main = max(tables, key=lambda t: (len(t.get("columns") or []), -tables.index(t)))
    main_cls = main["class_name"]
    main_label_n4j = f"`{prefix}__{main_cls}`"
    main_plural = _english_plural(main_cls.lower())

    _add(
        f"Show all {main_plural}",
        f"MATCH (n:{main_label_n4j}) RETURN n LIMIT 25",
        rf"\bshow\s+(?:me\s+)?(?:all\s+)?{main_plural}\b",
    )
    _add(
        f"Count {main_plural}",
        f"MATCH (n:{main_label_n4j}) RETURN count(n) AS total",
        rf"\b(?:how\s+many|count(?:\s+the)?)\s+{main_plural}\b",
    )
    # Strict: only emit "Top 10 by <PK>" when a real PK was inspected — don't
    # fake it from the first column or the example label is misleading.
    main_pk = main.get("primary_key")
    if main_pk:
        pk_n4j = f"`{prefix}__{main_pk}`"
        _add(
            f"Top 10 {main_plural} by {main_pk}",
            f"MATCH (n:{main_label_n4j}) "
            f"RETURN n.{pk_n4j} AS {main_pk}, n "
            f"ORDER BY {main_pk} LIMIT 10",
            None,
        )

    # ── Complex examples ───────────────────────────────────────────────────
    complex_specs = _complex_examples(schema, meta)
    for label, cypher in complex_specs:
        if not _add(label, cypher, None):
            break

    return examples, nl_rules


def _complex_examples(schema: dict, meta: dict) -> list[tuple[str, str]]:
    """Build the complex example pool. Returns a list of
    (label, cypher) tuples, longer than the caller's remaining budget;
    caller takes from the front until full.

    Priority groups (greedy concat — earlier groups always win the slot):
      (a) Relationship traversal — one per object property
      (b) Counts per related class — one per object property
      (c) Multi-hop A→B→C chains — at most one per starting class
      (d) Enum filter — one per (class, prop) that has enum_hints in the
          schema dict (description source); skipped for csv/postgres for
          now since their enum samples come from live Neo4j, which isn't
          loaded at bundle-build time."""
    prefix = meta["prefix"]
    tables = schema.get("tables") or []
    by_name = {t["class_name"]: t for t in tables}
    rels = _relationships_from_schema(schema)

    out: list[tuple[str, str]] = []

    # (a) Relationship traversal — one per object property.
    for domain, rel, rng in rels:
        d_table = by_name.get(domain)
        r_table = by_name.get(rng)
        if not d_table or not r_table:
            continue
        d_pk = _pk_of(d_table)
        r_pk = _pk_of(r_table)
        if not d_pk or not r_pk:
            continue
        d_n4j = f"`{prefix}__{domain}`"
        r_n4j = f"`{prefix}__{rng}`"
        rel_n4j = f"`{prefix}__{rel}`"
        d_pk_n4j = f"`{prefix}__{d_pk}`"
        r_pk_n4j = f"`{prefix}__{r_pk}`"
        # Return the nodes themselves alongside the keys so the graph
        # view has something to render — scalar-only results draw an
        # empty canvas.
        cypher = (
            f"MATCH (a:{d_n4j})-[:{rel_n4j}]->(b:{r_n4j}) "
            f"RETURN a.{d_pk_n4j} AS {d_pk}, b.{r_pk_n4j} AS {r_pk}, a, b "
            f"LIMIT 25"
        )
        label = f"{rng} with their {domain}"
        out.append((label, cypher))

    # (b) Counts per related class — answers "top X by Y".
    for domain, rel, rng in rels:
        d_table = by_name.get(domain)
        r_table = by_name.get(rng)
        if not d_table or not r_table:
            continue
        r_pk = _pk_of(r_table)
        if not r_pk:
            continue
        d_n4j = f"`{prefix}__{domain}`"
        r_n4j = f"`{prefix}__{rng}`"
        rel_n4j = f"`{prefix}__{rel}`"
        r_pk_n4j = f"`{prefix}__{r_pk}`"
        d_plural = _english_plural(domain.lower())
        alias_id = f"{rng.lower()}Id"
        alias_total = d_plural
        cypher = (
            f"MATCH (a:{d_n4j})-[:{rel_n4j}]->(b:{r_n4j}) "
            f"RETURN b.{r_pk_n4j} AS {alias_id}, count(a) AS {alias_total} "
            f"ORDER BY {alias_total} DESC LIMIT 10"
        )
        label = f"Top {rng}s by number of {d_plural}"
        out.append((label, cypher))

    # (c) Multi-hop A→B→C chains — at most one per starting class so
    # the chip strip doesn't fill up with near-identical paths off a
    # single hub class.
    seen_start: set[str] = set()
    for a, r1, b, r2, c in _find_two_hop_chains(rels):
        if a in seen_start:
            continue
        a_table = by_name.get(a)
        c_table = by_name.get(c)
        b_table = by_name.get(b)
        if not a_table or not c_table or not b_table:
            continue
        a_pk = _pk_of(a_table)
        c_pk = _pk_of(c_table)
        if not a_pk or not c_pk:
            continue
        a_n4j = f"`{prefix}__{a}`"
        b_n4j = f"`{prefix}__{b}`"
        c_n4j = f"`{prefix}__{c}`"
        r1_n4j = f"`{prefix}__{r1}`"
        r2_n4j = f"`{prefix}__{r2}`"
        a_pk_n4j = f"`{prefix}__{a_pk}`"
        c_pk_n4j = f"`{prefix}__{c_pk}`"
        cypher = (
            f"MATCH (a:{a_n4j})-[:{r1_n4j}]->(:{b_n4j})-[:{r2_n4j}]->(c:{c_n4j}) "
            f"RETURN a.{a_pk_n4j} AS {a_pk}, c.{c_pk_n4j} AS {c_pk}, a, c "
            f"LIMIT 25"
        )
        label = f"{a} connected to {c} via {b}"
        out.append((label, cypher))
        seen_start.add(a)

    # (d) Enum filter examples (description source only). For postgres/csv
    # the enum values come from live Neo4j (see schema_introspection
    # _sample_enum_values), which isn't running at bundle-build time —
    # those bundles fall back to traversal examples instead.
    if schema.get("source_kind") == "description":
        for table in tables:
            cls = table["class_name"]
            cls_n4j = f"`{prefix}__{cls}`"
            plural = _english_plural(cls.lower())
            for col in table.get("columns") or []:
                hints = col.get("enum_hints") or []
                if not hints:
                    continue
                # Pick the first hint as the filter literal — deterministic
                # across runs of the same schema. String-escape the value
                # in case the hint carries a stray single quote.
                literal = str(hints[0]).replace("\\", "\\\\").replace("'", "\\'")
                prop = col["name"]
                prop_n4j = f"`{prefix}__{prop}`"
                cypher = (
                    f"MATCH (n:{cls_n4j}) WHERE n.{prop_n4j} = '{literal}' "
                    f"RETURN n LIMIT 25"
                )
                label = f"{plural.capitalize()} where {prop} is {literal}"
                out.append((label, cypher))

    return out


def _humanise(camel_or_pascal: str) -> str:
    """workOrderId → 'Work Order Id'; WorkOrder → 'Work Order'.
    Used to derive sensible default rdfs:labels from generated identifiers."""
    s = re.sub(r"([A-Z])", r" \1", camel_or_pascal).strip()
    return s[:1].upper() + s[1:]


def _english_plural(word: str) -> str:
    """Inverse of _singular — naïve English plural for use in example labels.
    'order' → 'orders', 'address' → 'addresses', 'company' → 'companies'."""
    if not word:
        return word
    lower = word.lower()
    if lower.endswith("y") and len(lower) > 1 and lower[-2] not in "aeiou":
        return word[:-1] + "ies"
    if lower.endswith(("s", "x", "z", "ch", "sh")):
        return word + "es"
    return word + "s"


def _build_pull_adapter(datasource_id: str, table: dict, index: int) -> dict:
    """Produce one stage-4 adapter for a Postgres table. SQL uses the
    ORIGINAL Postgres column name on the SELECT side and aliases it to
    the camelCase property name on the AS side, so:
      - Postgres can resolve the column (it only knows the original name)
      - psycopg returns the row dict keyed by the camelCase alias, which
        matches the property name in the ontology
    Falls back to the property name on the SELECT side for sources
    (CSV) that don't carry sql_name.
    """
    if not table.get("columns"):
        raise ValueError(
            f"Table {table.get('name')!r}: cannot build pull adapter with zero columns."
        )
    select_parts = []
    for c in table["columns"]:
        select_side = _quote_sql_ident(c.get("sql_name") or c["name"])
        alias_side = _quote_sql_ident(c["name"])
        select_parts.append(f"{select_side} AS {alias_side}")
    sql = f"SELECT {', '.join(select_parts)}\nFROM {_quote_sql_ident(table['name'])}\nLIMIT 1000"
    pk = table.get("primary_key") or table["columns"][0]["name"]
    return {
        "adapter_id": f"PG-{table['class_name'].upper()}-{index:03d}",
        "source_system": table["name"].upper(),
        "protocol": "postgres",
        "sync_mode": "FULL",
        "target_class": table["class_name"],
        "match_property": "sourceSystem",
        "pull": {
            "datasource": datasource_id,
            "sql": sql,
            "label": table["class_name"],
            "key_property": pk,
        },
    }


def _build_data_ttl(schema: dict, meta: dict, onto: str | None = None) -> tuple[str, int]:
    """For Postgres → empty (pull adapters do the loading).
    For CSV → emit one node per sample row that the inspector cached.
    For description → synthesize plausible seed data from the ontology, since
    there's no datasource to pull from and no sample rows; an empty graph would
    leave the Query Console dead on a freshly-built bundle."""
    if schema["source_kind"] == "postgres":
        return "# empty — datasource pull adapters populate at hydration time\n", 0

    if schema["source_kind"] == "description":
        if not onto:
            return "# empty — no ontology available for synthetic seed\n", 0
        from pipeline.data_generator import generate_data
        # Re-collect enum hints by class so the data generator picks the
        # user-supplied vocabulary (APPROVED/REJECTED/...) instead of the
        # generic _STATUSES default. Mirrors the carry-through done in
        # _build_manifest so the manifest and the data agree on values.
        enum_hints_by_class: dict[str, dict[str, list[str]]] = {}
        for table in schema["tables"]:
            cls = table["class_name"]
            for col in table.get("columns", []) or []:
                hints = col.get("enum_hints") or []
                if hints:
                    enum_hints_by_class.setdefault(cls, {})[col["name"]] = list(hints)
        ttl, summ = generate_data(
            onto, meta["namespace"], count=10, seed=42,
            enum_hints_by_class=enum_hints_by_class or None,
        )
        return (ttl or "# no classes — empty data.ttl\n"), int(summ.get("total_nodes", 0) or 0)

    # CSV: bundled rows live under each table's `sample_rows` field.
    lines = [
        f"@prefix {meta['prefix']}: <{meta['namespace']}> .",
        f"@prefix rdf: <http://www.w3.org/1999/02/22-rdf-syntax-ns#> .",
        f"@prefix rdfs: <http://www.w3.org/2000/01/rdf-schema#> .",
        f"@prefix xsd: <http://www.w3.org/2001/XMLSchema#> .",
        "",
    ]
    triples = 0
    for table in schema["tables"]:
        cls = table["class_name"]
        cols_by_name = {c["name"]: c for c in table.get("columns", [])}
        pk_col = table.get("primary_key") or (table["columns"][0]["name"] if table["columns"] else None)
        if not pk_col:
            continue
        for i, row in enumerate(table.get("sample_rows") or [], start=1):
            key_val = row.get(pk_col)
            if key_val is None or str(key_val).strip() == "":
                continue
            # Derive a safe local-name URI from the key value. Strip anything
            # that isn't URI-safe.
            safe_key = re.sub(r"[^A-Za-z0-9_-]", "_", str(key_val))[:63] or f"row{i}"
            subject = f"{meta['prefix']}:{cls}_{safe_key}"
            statements = [f"a {meta['prefix']}:{cls}", f'rdfs:label "{cls} {safe_key}"']
            for col_name, value in row.items():
                if value is None or str(value).strip() == "":
                    continue
                col_meta = cols_by_name.get(col_name)
                if not col_meta:
                    continue
                lit = _escape_ttl_literal(value, col_meta["xsd_type"])
                if lit is None:
                    continue
                statements.append(f"{meta['prefix']}:{col_name} {lit}")
                triples += 1
            lines.append(subject + "\n    " + " ;\n    ".join(statements) + " .")
            triples += 1   # rdf:type itself
    return "\n".join(lines) + "\n", triples


def _summarize(schema: dict, obj_count: int, data_triples: int) -> dict:
    classes = len(schema.get("tables") or [])
    dt_props = sum(len(t.get("columns") or []) for t in schema["tables"])
    return {
        "classes": classes,
        "datatype_properties": dt_props,
        "object_properties": obj_count,
        "data_triples": data_triples,
    }


# ── Tiny English singular/plural helper ─────────────────────────────────────
# Avoids pulling in inflect/pyenchant for one operation. Handles the
# common cases (orders→Order, customers→Customer, addresses→Address,
# companies→Company); falls back to the input unchanged otherwise.

def _singular(word: str) -> str:
    w = word.strip()
    if not w:
        return w
    lower = w.lower()
    if lower.endswith("ies") and len(lower) > 3:
        return w[:-3] + "y"
    if lower.endswith("ses") or lower.endswith("xes") or lower.endswith("zes"):
        return w[:-2]
    # Don't strip 's' from Latin/Greek -us / -is endings (status, analysis,
    # bus, focus) or from -ss (class, glass).
    if lower.endswith("s") and not lower.endswith(("ss", "us", "is")):
        return w[:-1]
    return w


def singularise_pascal(name: str) -> str:
    """Convert a snake_case table name to a singularised PascalCase class
    name. e.g. 'work_orders' → 'WorkOrder', 'customer_addresses' → 'CustomerAddress'."""
    parts = re.split(r"[_\W]+", name.strip())
    parts = [p for p in parts if p]
    if not parts:
        return name
    parts[-1] = _singular(parts[-1])
    return "".join(p[:1].upper() + p[1:].lower() for p in parts)
