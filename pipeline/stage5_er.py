"""Stage 5 — Entity Resolution: run manifest-declared rules and merge duplicates.

Each rule is a Cypher query that returns rows with at minimum
  canonical_eid, duplicate_eid, canonical_id, duplicate_id

For every row, this stage copies merge metadata onto the canonical node and
DETACH DELETEs the duplicate. If the manifest has no rules, the stage is a no-op.
"""
from db import run_query, run_write


def run_entity_resolution(ctx: dict) -> list[str]:
    logs = []
    use_case = ctx["use_case"]
    rules = use_case.manifest.stage5_er_rules

    if not rules:
        logs.append("INFO  No ER rules declared in manifest, skipping")
        return logs

    # Count nodes of any in-scope class before/after for a meaningful summary
    in_scope_labels = [use_case.label(c) for c in use_case.manifest.in_scope_classes if c != "IngestionAdapter"]
    before = _count_in_scope(in_scope_labels)
    logs.append(f"INFO  In-scope nodes before ER: {before}")

    p_merged_from   = use_case.prop("mergedFrom")
    p_merge_method  = use_case.prop("mergeMethod")
    p_merge_conf    = use_case.prop("mergeConfidence")

    total_merged = 0
    for rule in rules:
        try:
            pairs = run_query(rule.cypher)
        except Exception as exc:
            logs.append(f"WARN  {rule.id} cypher failed: {exc}")
            continue

        merged_for_rule = 0
        for pair in pairs:
            canonical_eid = pair.get("canonical_eid")
            duplicate_eid = pair.get("duplicate_eid")
            canonical_id  = pair.get("canonical_id", "?")
            duplicate_id  = pair.get("duplicate_id", "?")
            if not canonical_eid or not duplicate_eid or canonical_eid == duplicate_eid:
                continue
            try:
                run_write(
                    f"""
                    MATCH (a) WHERE elementId(a) = $canonical_eid
                    MATCH (b) WHERE elementId(b) = $duplicate_eid
                    SET a.`{p_merged_from}`  = $duplicate_id,
                        a.`{p_merge_method}` = $rule_id,
                        a.`{p_merge_conf}`   = $confidence
                    DETACH DELETE b
                    """,
                    {
                        "canonical_eid": canonical_eid,
                        "duplicate_eid": duplicate_eid,
                        "duplicate_id":  duplicate_id,
                        "rule_id":       rule.id,
                        "confidence":    rule.confidence,
                    },
                )
                merged_for_rule += 1
            except Exception as exc:
                logs.append(f"WARN  {rule.id} merge {duplicate_id} -> {canonical_id} failed: {exc}")

        total_merged += merged_for_rule
        logs.append(f"PASS  {rule.id} ({rule.description}): {merged_for_rule} pair(s) merged at confidence {rule.confidence:.2f}")

    after = _count_in_scope(in_scope_labels)
    logs.append(f"PASS  In-scope nodes after ER: {after} · merged: {total_merged} · HRQ: 0")
    return logs


def _count_in_scope(labels: list[str]) -> int:
    if not labels:
        return 0
    union_clauses = " UNION ALL ".join(
        f"MATCH (n:`{label}`) RETURN n" for label in labels
    )
    rows = run_query(f"CALL {{ {union_clauses} }} RETURN count(n) AS n")
    return rows[0]["n"] if rows else 0
