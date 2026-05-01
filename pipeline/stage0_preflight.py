"""Stage 0 — Preflight Check: verify Neo4j connectivity, plugins, and bundle files."""

from db import get_driver, run_query


def preflight(ctx: dict) -> list[str]:
    logs = []
    use_case = ctx["use_case"]

    driver = get_driver()
    with driver.session() as session:
        version = session.run("CALL dbms.components() YIELD versions RETURN versions[0] AS v").single()["v"]
    logs.append(f"PASS  Neo4j connected — version {version}")

    try:
        run_query("CALL n10s.graphconfig.show()")
        logs.append("PASS  n10s plugin available")
    except Exception:
        raise RuntimeError("n10s plugin not found. Enable it in your Neo4j instance.")

    try:
        run_query("RETURN apoc.version() AS v")
        logs.append("PASS  APOC plugin available")
    except Exception:
        raise RuntimeError("APOC plugin not found. Enable it in your Neo4j instance.")

    logs.append(f"PASS  Bundle: {use_case.manifest.name} ({use_case.slug})")
    for label, path in [("ontology", use_case.ontology_path), ("data", use_case.data_path)]:
        if not path.exists():
            raise FileNotFoundError(f"{label} TTL not found in bundle: {path}")
        size_kb = path.stat().st_size // 1024
        logs.append(f"PASS  {label} TTL found — {size_kb} KB ({path.name})")

    return logs
