"""Stage 2 — OWL2 Schema Load: import ontology TTL and create constraints/indexes."""

import os
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
    ttl_path = os.path.abspath(s.ontology_ttl_path)

    # Import ontology
    result = run_query(
        "CALL n10s.onto.import.fetch($url, 'Turtle') YIELD triplesLoaded RETURN triplesLoaded",
        {"url": f"file:///{ttl_path}"},
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
