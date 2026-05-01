"""Stage 1 — Wipe + n10s Init: clear database and configure n10s namespaces."""

from db import run_write


def wipe_and_init(ctx: dict) -> list[str]:
    logs = []
    use_case = ctx["use_case"]

    run_write("MATCH (n) DETACH DELETE n")
    logs.append("PASS  Database cleared")

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
