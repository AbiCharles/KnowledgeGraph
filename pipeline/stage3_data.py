"""Stage 3 — Test Data Load: import WorkOrder RDF/TTL test dataset."""

from db import run_query


def load_data(ctx: dict) -> list[str]:
    logs = []
    s = ctx["settings"]

    with open(s.data_ttl_path, "r", encoding="utf-8") as f:
        payload = f.read()

    result = run_query(
        "CALL n10s.rdf.import.inline($payload, 'Turtle') YIELD triplesLoaded RETURN triplesLoaded",
        {"payload": payload},
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
