"""Stage 6 — Validation: run manifest-declared checks (critical + warning).

Each check has a `kind` that selects a small handler. Critical failures raise
RuntimeError to fail the pipeline; warnings are logged. If the manifest has no
checks, a generic minimal suite runs (count >= 1 per in-scope class).
"""
from db import run_query


def validate(ctx: dict) -> list[str]:
    logs = []
    use_case = ctx["use_case"]
    checks = use_case.manifest.stage6_checks or _generic_checks(use_case)
    if not checks:
        logs.append("WARN  No checks declared and no in-scope classes — nothing to validate")
        return logs

    failures = []
    for check in checks:
        try:
            ok, msg = _run_check(use_case, check)
        except Exception as exc:
            ok, msg = False, f"check failed to execute: {exc}"

        prefix = "PASS" if ok else ("WARN" if check.severity == "warning" else "FAIL")
        line = f"{prefix}  {check.id} {msg}"
        logs.append(line)
        if not ok and check.severity == "critical":
            failures.append(f"{check.id} {msg}")

    if failures:
        raise RuntimeError("Validation failed: " + "; ".join(failures))

    return logs


def _scalar(rows: list[dict], col: str):
    """Pull a single scalar from rows[0][col] safely. Returns None if missing."""
    if not rows:
        return None
    return rows[0].get(col)


def _strict_bool(v) -> bool:
    """True only for genuine booleans / numeric truthy values; refuses ambiguous strings."""
    if isinstance(v, bool):
        return v
    if isinstance(v, (int, float)):
        return bool(v)
    if v is None:
        return False
    raise ValueError(f"check returned non-boolean 'passed' value of type {type(v).__name__}: {v!r}")


def _run_check(use_case, check) -> tuple[bool, str]:
    if check.kind == "count_at_least":
        label = use_case.label(check.label)
        n = _scalar(run_query(f"MATCH (x:`{label}`) RETURN count(x) AS n"), "n") or 0
        passed = n >= (check.threshold or 0)
        return passed, f"count({check.label})={n} {'>=' if passed else '<'} {check.threshold}"

    if check.kind == "count_equals":
        label = use_case.label(check.label)
        n = _scalar(run_query(f"MATCH (x:`{label}`) RETURN count(x) AS n"), "n") or 0
        passed = n == check.value
        return passed, f"count({check.label})={n} {'==' if passed else '!='} {check.value}"

    if check.kind == "no_duplicates":
        label = use_case.label(check.label)
        prop = use_case.prop(check.property)
        n = _scalar(
            run_query(
                f"MATCH (x:`{label}`) WITH x.`{prop}` AS k, count(*) AS c WHERE c > 1 RETURN count(*) AS dup"
            ),
            "dup",
        ) or 0
        return n == 0, f"{n} duplicate {check.label}.{check.property}"

    if check.kind == "no_orphans":
        label = use_case.label(check.label)
        n = _scalar(run_query(f"MATCH (x:`{label}`) WHERE NOT (x)--() RETURN count(x) AS n"), "n") or 0
        return n == 0, f"{n} orphaned {check.label} nodes"

    if check.kind == "cypher":
        if not check.cypher:
            return False, "cypher check missing query"
        rows = run_query(check.cypher)
        if not rows:
            return False, "cypher check returned zero rows (expected one row with 'passed')"
        if "passed" not in rows[0]:
            return False, f"cypher check did not return 'passed' column (got {list(rows[0].keys())})"
        if len(rows) > 1:
            # All-rows-must-pass semantics so multi-row checks aren't silently accepted.
            ok_all = all(_strict_bool(r.get("passed")) for r in rows)
            extras = f"({len(rows)} rows)"
            return ok_all, (check.description or "cypher check") + " " + extras
        passed = _strict_bool(rows[0]["passed"])
        extras = ", ".join(f"{k}={v}" for k, v in rows[0].items() if k != "passed")
        return passed, (check.description or "cypher check") + (f" ({extras})" if extras else "")

    return False, f"unknown check kind {check.kind!r}"


def _generic_checks(use_case):
    """Default suite when manifest has no checks: count >= 1 for each
    in-scope class.

    Smart severity: a class with at least one declared datatype property
    in the ontology gets severity=critical (a missing instance is a real
    ingestion bug — "I forgot to load Orders"). A class with zero
    datatype properties is treated as a marker class (taxonomy / parent
    type / linter test fixture) and gets severity=warning so an empty
    count surfaces as a WARN log line without failing the pipeline.

    Rationale: marker classes can't carry meaningful data anyway, and
    requiring fake bare instances just to satisfy validation defeats
    the point. Classes with properties stay strict — a missing instance
    of a real entity should still halt the pipeline.
    """
    from pipeline.use_case import CheckSpec

    # Walk the ontology directly via rdflib to count datatype properties
    # PER CLASS (only those whose rdfs:domain == this class). We avoid
    # pipeline.schema_introspection.schema_summary because that helper
    # promotes unscoped properties into every class's bucket for
    # autocomplete purposes — that would defeat marker-class detection.
    introspect_failed = False
    classes_with_props: set[str] = set()
    try:
        from rdflib import Graph, OWL, RDF, RDFS, URIRef
        g = Graph()
        g.parse(str(use_case.ontology_path), format="turtle")
        for prop in g.subjects(RDF.type, OWL.DatatypeProperty):
            for domain in g.objects(prop, RDFS.domain):
                if isinstance(domain, URIRef):
                    s = str(domain)
                    local = s.rsplit("#", 1)[-1].rsplit("/", 1)[-1]
                    classes_with_props.add(local)
    except Exception:
        # Corrupt TTL would have failed earlier stages, but if it
        # somehow reaches here be conservative — every check critical
        # rather than silently degrading every one to warning.
        introspect_failed = True

    checks = []
    for i, cls in enumerate(use_case.manifest.in_scope_classes):
        is_marker = (not introspect_failed) and (cls not in classes_with_props)
        sev = "warning" if is_marker else "critical"
        suffix = " (warning — class has no datatype properties)" if is_marker else ""
        checks.append(
            CheckSpec(
                id=f"VC-AUTO-{i+1:02d}",
                kind="count_at_least",
                severity=sev,
                label=cls,
                threshold=1,
                description=f"At least one {cls} node{suffix}",
            )
        )
    return checks
