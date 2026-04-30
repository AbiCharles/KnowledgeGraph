"""Stage 2 — OWL2 Schema Load: import ontology TTL and create constraints/indexes."""

from db import run_write, run_query


CONSTRAINTS = [
    ("wo_status",   "kf-mfg__WorkOrder",       "kf-mfg__woStatus"),
    ("wo_type",     "kf-mfg__WorkOrder",       "kf-mfg__woType"),
    ("wo_priority", "kf-mfg__WorkOrder",       "kf-mfg__woPriority"),
]

INDEXES = [
    ("kf-mfg__WorkOrder",       "kf-mfg__workOrderId"),
    ("kf-mfg__Equipment",       "kf-mfg__equipmentId"),
    ("kf-mfg__Technician",      "kf-mfg__technicianId"),
    ("kf-mfg__CompliancePolicy","kf-mfg__policyId"),
    ("kf-mfg__ProductionLine",  "kf-mfg__lineId"),
]


def load_schema(ctx: dict) -> list[str]:
    logs = []
    s = ctx["settings"]

    with open(s.ontology_ttl_path, "r", encoding="utf-8") as f:
        payload = f.read()

    result = run_query(
        "CALL n10s.onto.import.inline($payload, 'Turtle') YIELD triplesLoaded RETURN triplesLoaded",
        {"payload": payload},
    )
    triples = result[0]["triplesLoaded"] if result else 0
    logs.append(f"PASS  Ontology loaded — {triples} triples")

    # Constraints
    for name, label, prop in CONSTRAINTS:
        run_write(
            f"CREATE CONSTRAINT {name} IF NOT EXISTS "
            f"FOR (n:`{label}`) REQUIRE n.`{prop}` IS NOT NULL"
        )
    logs.append(f"PASS  {len(CONSTRAINTS)}/3 constraints created")

    # Indexes
    for label, prop in INDEXES:
        run_write(
            f"CREATE INDEX IF NOT EXISTS FOR (n:`{label}`) ON (n.`{prop}`)"
        )
    logs.append(f"PASS  {len(INDEXES)}/5 indexes created")

    return logs
