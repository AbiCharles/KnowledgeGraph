"""Stage 2 — OWL2 Schema Load: import ontology TTL and create constraints/indexes."""

from db import run_write, run_query


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
        run_write(
            f"CREATE CONSTRAINT {use_case.slug.replace('-','_')}_{spec.label}_{spec.property} IF NOT EXISTS "
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
