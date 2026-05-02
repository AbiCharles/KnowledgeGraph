"""Stage 1 — Wipe + n10s Init: clear database and configure n10s namespaces.

Schema-level constraints/indexes from prior pipeline runs of OTHER bundles
are dropped so the new bundle's stage 2 can install its own without conflict.
We target ONLY constraints/indexes whose names don't match what the active
bundle's stage 2 is about to create — this preserves operator-managed schema
that lives outside the manifest workflow.
"""
import re

from db import run_query, run_write


_BUNDLE_NAME_RE = re.compile(r"^([a-z0-9_-]+)_[A-Za-z0-9_]+_[A-Za-z0-9_]+(_idx)?$")


def wipe_and_init(ctx: dict) -> list[str]:
    logs = []
    use_case = ctx["use_case"]

    run_write("MATCH (n) DETACH DELETE n")
    logs.append("PASS  Database cleared")

    # Drop bundle-owned schema (anything named like `<slug>_<class>_<prop>` or
    # `<slug>_<class>_<prop>_idx`) so a different bundle's manifest can install
    # its own constraints. Operator-added constraints with non-bundle names are
    # left alone.
    dropped_c = _drop_bundle_schema("CONSTRAINTS", logs)
    dropped_i = _drop_bundle_schema("INDEXES",     logs, extra_filter=" WHERE type <> 'LOOKUP'")
    logs.append(
        f"PASS  Dropped {dropped_c} bundle constraint(s) and {dropped_i} bundle index(es); "
        f"operator-managed schema preserved"
    )

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


def _drop_bundle_schema(kind: str, logs: list, extra_filter: str = "") -> int:
    """Drop only schema items whose names match the bundle <slug>_<class>_<prop>(_idx)?
    pattern. Returns the count of items actually dropped. `kind` is "CONSTRAINTS"
    or "INDEXES"."""
    drop_kw = "CONSTRAINT" if kind == "CONSTRAINTS" else "INDEX"
    dropped = 0
    try:
        rows = run_query(f"SHOW {kind} YIELD name{extra_filter}") or []
    except Exception as exc:
        logs.append(f"WARN  Could not enumerate {kind.lower()}: {exc}")
        return 0
    for r in rows:
        name = r.get("name")
        if not name or not _BUNDLE_NAME_RE.match(name):
            continue
        try:
            run_write(f"DROP {drop_kw} `{name}` IF EXISTS")
            dropped += 1
        except Exception as exc:
            logs.append(f"WARN  Could not drop {drop_kw.lower()} {name}: {exc}")
    return dropped
