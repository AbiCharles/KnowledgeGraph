"""Stage 3 — Test Data Load: import WorkOrder RDF/TTL test dataset."""

import os
from db import run_query


def load_data(ctx: dict) -> list[str]:
    logs = []
    s = ctx["settings"]
    ttl_path = os.path.abspath(s.data_ttl_path)

    result = run_query(
        "CALL n10s.rdf.import.fetch($url, 'Turtle') YIELD triplesLoaded RETURN triplesLoaded",
        {"url": f"file:///{ttl_path}"},
    )
    triples = result[0]["triplesLoaded"] if result else 0
    logs.append(f"PASS  Test data loaded — {triples} triples")

    # Count imported nodes
    counts = run_query("""
        MATCH (n)
        RETURN labels(n)[0] AS label, count(n) AS cnt
        ORDER BY label
    """)
    for row in counts:
        logs.append(f"INFO  {row['label']}: {row['cnt']} nodes")

    return logs
