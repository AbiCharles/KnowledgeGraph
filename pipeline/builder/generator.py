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
    if schema.get("source_kind") not in ("postgres", "csv"):
        raise ValueError(
            f"source_kind must be 'postgres' or 'csv', got {schema.get('source_kind')!r}"
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

    # Pass 1: classes
    for table in schema["tables"]:
        onto, _ = add_class(
            onto, meta["namespace"], table["class_name"],
            description=f"Generated from {schema['source_kind']} table {table['name']!r}",
        )

    # Pass 2: datatype properties — one per column.
    for table in schema["tables"]:
        for col in table.get("columns", []):
            try:
                onto, _ = add_datatype_property(
                    onto, meta["namespace"],
                    local_name=col["name"],
                    domain_class=table["class_name"],
                    xsd_range=col["xsd_type"],
                )
            except ValueError as exc:
                # Ignore duplicate-property errors (same col name on two
                # tables). The first wins; surface this in the summary.
                if "already exists" not in str(exc):
                    raise

    # Pass 3: object properties from foreign keys (Postgres only).
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

    # 2. Build the manifest. Postgres source pre-populates datasources +
    #    pull adapters so the bundle is hydration-ready immediately.
    manifest_dict = _build_manifest(schema, meta)

    # 3. Validate the manifest against the production Pydantic model BEFORE
    #    returning anything. Ensures any bundle the wizard emits will load.
    Manifest(**manifest_dict)

    # 4. Build data.ttl. Postgres → empty (pulls handle loading);
    #    CSV → seed with one node per row.
    data_ttl, data_triples = _build_data_ttl(schema, meta)

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
    adapters; CSV stays minimal."""
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
    return manifest


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


def _build_data_ttl(schema: dict, meta: dict) -> tuple[str, int]:
    """For Postgres → empty (pull adapters do the loading).
    For CSV → emit one node per sample row that the inspector cached."""
    if schema["source_kind"] == "postgres":
        return "# empty — datasource pull adapters populate at hydration time\n", 0

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
