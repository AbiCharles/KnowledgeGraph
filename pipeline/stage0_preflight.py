"""Stage 0 — Preflight Check: verify Neo4j connectivity, plugins, and TTL files."""

import os
from db import get_driver, run_query


def preflight(ctx: dict) -> list[str]:
    logs = []
    s = ctx["settings"]

    # 1. Neo4j connectivity
    driver = get_driver()
    with driver.session() as session:
        version = session.run("CALL dbms.components() YIELD versions RETURN versions[0] AS v").single()["v"]
    logs.append(f"PASS  Neo4j connected — version {version}")

    # 2. n10s plugin
    try:
        run_query("CALL n10s.graphconfig.show()")
        logs.append("PASS  n10s plugin available")
    except Exception:
        raise RuntimeError("n10s plugin not found. Enable it in your AuraDB instance.")

    # 3. APOC plugin
    try:
        run_query("RETURN apoc.version() AS v")
        logs.append("PASS  APOC plugin available")
    except Exception:
        raise RuntimeError("APOC plugin not found. Enable it in your AuraDB instance.")

    # 4. TTL files
    for label, path in [("ontology", s.ontology_ttl_path), ("data", s.data_ttl_path)]:
        if not os.path.exists(path):
            raise FileNotFoundError(f"{label} TTL file not found: {path}")
        size_kb = os.path.getsize(path) // 1024
        logs.append(f"PASS  {label} TTL found — {size_kb} KB ({path})")

    return logs
