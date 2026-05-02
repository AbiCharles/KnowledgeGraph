"""Stage 5 — Entity Resolution: run manifest-declared rules and merge duplicates.

Each rule is a Cypher query that returns rows with at minimum
  canonical_eid, duplicate_eid, canonical_id, duplicate_id

For every row, this stage copies merge metadata onto the canonical node and
DETACH DELETEs the duplicate. If the manifest has no rules, the stage is a no-op.

The rule query and its consequent merge run inside a single Neo4j session so
the elementIds the rule returns remain valid for the merge — Neo4j only
guarantees elementId stability within a session/transaction boundary.
"""
from db import run_in_session, run_query


def run_entity_resolution(ctx: dict) -> list[str]:
    logs = []
    use_case = ctx["use_case"]
    rules = use_case.manifest.stage5_er_rules

    if not rules:
        logs.append("INFO  No ER rules declared in manifest, skipping")
        return logs

    in_scope_labels = [
        use_case.label(c) for c in use_case.manifest.in_scope_classes
        if c != "IngestionAdapter"
    ]
    before = _count_in_scope(in_scope_labels)
    logs.append(f"INFO  In-scope nodes before ER: {before}")

    p_merged_from   = use_case.prop("mergedFrom")
    p_merge_method  = use_case.prop("mergeMethod")
    p_merge_conf    = use_case.prop("mergeConfidence")

    total_merged = 0
    for rule in rules:
        merged_for_rule, rule_logs = _run_rule(rule, p_merged_from, p_merge_method, p_merge_conf)
        logs.extend(rule_logs)
        total_merged += merged_for_rule
        logs.append(
            f"PASS  {rule.id} ({rule.description}): {merged_for_rule} pair(s) "
            f"merged at confidence {rule.confidence:.2f}"
        )

    after = _count_in_scope(in_scope_labels)
    logs.append(f"PASS  In-scope nodes after ER: {after} · merged: {total_merged} · HRQ: 0")
    return logs


def _run_rule(rule, p_merged_from: str, p_merge_method: str, p_merge_conf: str):
    """Run one ER rule in a single session: read pairs, then merge each.

    Same-session execution is required — Neo4j elementIds are only stable
    inside a single session/transaction, so reading then writing across two
    separate sessions could (in theory, under store compaction or concurrent
    writers) merge the wrong nodes.
    """
    logs: list[str] = []
    merge_cypher = (
        f"MATCH (a) WHERE elementId(a) = $canonical_eid\n"
        f"MATCH (b) WHERE elementId(b) = $duplicate_eid\n"
        f"SET a.`{p_merged_from}`  = $duplicate_id,\n"
        f"    a.`{p_merge_method}` = $rule_id,\n"
        f"    a.`{p_merge_conf}`   = $confidence\n"
        f"DETACH DELETE b"
    )

    def _do(session) -> int:
        merged = 0
        try:
            pairs = list(session.run(rule.cypher))
        except Exception as exc:
            logs.append(f"WARN  {rule.id} cypher failed: {exc}")
            return 0
        for pair in pairs:
            d = dict(pair)
            canonical_eid = d.get("canonical_eid")
            duplicate_eid = d.get("duplicate_eid")
            canonical_id  = d.get("canonical_id", "?")
            duplicate_id  = d.get("duplicate_id", "?")
            if not canonical_eid or not duplicate_eid or canonical_eid == duplicate_eid:
                continue
            try:
                session.run(merge_cypher, {
                    "canonical_eid": canonical_eid,
                    "duplicate_eid": duplicate_eid,
                    "duplicate_id":  duplicate_id,
                    "rule_id":       rule.id,
                    "confidence":    rule.confidence,
                }).consume()
                merged += 1
            except Exception as exc:
                logs.append(f"WARN  {rule.id} merge {duplicate_id} -> {canonical_id} failed: {exc}")
        return merged

    return run_in_session(_do), logs


def _count_in_scope(labels: list[str]) -> int:
    """Count distinct nodes carrying any of the given labels.

    Per-label query keeps each query small; client-side dedupe by elementId
    handles nodes carrying multiple in-scope labels (n10s SHORTEN sometimes
    leaves both Resource and the typed label).
    """
    if not labels:
        return 0
    seen: set[str] = set()
    for label in labels:
        rows = run_query(f"MATCH (n:`{label}`) RETURN elementId(n) AS eid")
        for r in rows:
            seen.add(r["eid"])
    return len(seen)
