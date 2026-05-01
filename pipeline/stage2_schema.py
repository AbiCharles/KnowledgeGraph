"""Stage 2 — OWL2 Schema Load: import ontology TTL and create constraints/indexes."""

import hashlib
import re

from db import run_write, run_query


_ID_SAFE = re.compile(r"[^A-Za-z0-9_]")


def _constraint_name(slug: str, label: str, prop: str) -> str:
    """Build a Neo4j constraint identifier that always fits the 63-char limit.

    Sanitises non-identifier chars and, if the natural name overflows, appends
    a short hash so distinct (slug, label, property) triples never collide.
    """
    base = _ID_SAFE.sub("_", f"{slug}_{label}_{prop}")
    if len(base) <= 63:
        return base
    digest = hashlib.sha1(base.encode()).hexdigest()[:8]
    return base[: 63 - 9] + "_" + digest


def load_schema(ctx: dict) -> list[str]:
    logs = []
    use_case = ctx["use_case"]
    manifest = use_case.manifest

    with open(use_case.ontology_path, "r", encoding="utf-8") as f:
        payload = f.read()

    result = run_query(
        "CALL n10s.onto.import.inline($payload, 'Turtle') YIELD triplesLoaded RETURN triplesLoaded",
        {"payload": payload},
    )
    triples = result[0]["triplesLoaded"] if result else 0
    logs.append(f"PASS  Ontology loaded — {triples} triples")

    for spec in manifest.stage2_constraints:
        label = use_case.label(spec.label)
        prop = use_case.prop(spec.property)
        cname = _constraint_name(use_case.slug, spec.label, spec.property)
        run_write(
            f"CREATE CONSTRAINT {cname} IF NOT EXISTS "
            f"FOR (n:`{label}`) REQUIRE n.`{prop}` IS NOT NULL"
        )
    logs.append(f"PASS  {len(manifest.stage2_constraints)}/{len(manifest.stage2_constraints)} constraints created")

    for spec in manifest.stage2_indexes:
        label = use_case.label(spec.label)
        prop = use_case.prop(spec.property)
        run_write(
            f"CREATE INDEX IF NOT EXISTS FOR (n:`{label}`) ON (n.`{prop}`)"
        )
    logs.append(f"PASS  {len(manifest.stage2_indexes)}/{len(manifest.stage2_indexes)} indexes created")

    return logs
