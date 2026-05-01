"""Stage 1 — Wipe + n10s Init: clear database and configure n10s namespaces."""

from db import run_query, run_write


def wipe_and_init(ctx: dict) -> list[str]:
    logs = []
    use_case = ctx["use_case"]

    run_write("MATCH (n) DETACH DELETE n")
    logs.append("PASS  Database cleared")

    # Drop schema left by previous bundles so a different bundle's manifest
    # can install its own constraints/indexes without conflict. The n10s
    # uniqueness constraint and any other reserved-name constraint are
    # recreated below as needed.
    dropped_c = 0
    try:
        rows = run_query("SHOW CONSTRAINTS YIELD name") or []
        for r in rows:
            name = r.get("name")
            if not name:
                continue
            run_write(f"DROP CONSTRAINT `{name}` IF EXISTS")
            dropped_c += 1
    except Exception as exc:
        logs.append(f"WARN  Could not enumerate constraints to drop: {exc}")
    dropped_i = 0
    try:
        rows = run_query("SHOW INDEXES YIELD name, type WHERE type <> 'LOOKUP'") or []
        for r in rows:
            name = r.get("name")
            if not name:
                continue
            run_write(f"DROP INDEX `{name}` IF EXISTS")
            dropped_i += 1
    except Exception as exc:
        logs.append(f"WARN  Could not enumerate indexes to drop: {exc}")
    logs.append(f"PASS  Dropped {dropped_c} constraint(s) and {dropped_i} index(es) from prior bundles")

    run_write(
        "CREATE CONSTRAINT n10s_unique_uri IF NOT EXISTS "
        "FOR (r:Resource) REQUIRE r.uri IS UNIQUE"
    )
    logs.append("PASS  n10s Resource(uri) uniqueness constraint ensured")

    run_write("""
        CALL n10s.graphconfig.init({
            handleVocabUris: 'SHORTEN',
            handleMultival: 'OVERWRITE',
            handleRDFTypes: 'LABELS_AND_NODES',
            keepLangTag: false,
            keepCustomDataTypes: false
        })
    """)
    logs.append("PASS  n10s graphconfig initialised in SHORTEN mode")

    prefixes = {use_case.manifest.prefix: use_case.manifest.namespace}
    prefixes.update(use_case.manifest.extra_prefixes)
    for prefix, uri in prefixes.items():
        run_write(
            "CALL n10s.nsprefixes.add($prefix, $uri)",
            {"prefix": prefix, "uri": uri},
        )
    logs.append(f"PASS  {len(prefixes)}/{len(prefixes)} namespace prefixes registered")

    return logs
