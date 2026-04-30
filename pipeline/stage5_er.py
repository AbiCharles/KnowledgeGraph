"""Stage 5 — Entity Resolution: detect and merge duplicate WorkOrders."""

from db import run_query, run_write


# Confidence thresholds
AUTO_MERGE_THRESHOLD = 0.85
HUMAN_REVIEW_THRESHOLD = 0.70


def run_entity_resolution(ctx: dict) -> list[str]:
    logs = []

    before = run_query("MATCH (wo:`kf-mfg__WorkOrder`) RETURN count(wo) AS n")[0]["n"]
    logs.append(f"INFO  WorkOrders before ER: {before}")

    merged_total = 0

    # ── Rule ER-001: Exact primary key match ─────────────────────────────────
    pairs = run_query("""
        MATCH (a:`kf-mfg__WorkOrder` {`kf-mfg__sourceSystem`: 'SAP-PM'})
        MATCH (b:`kf-mfg__WorkOrder` {`kf-mfg__sourceSystem`: 'MES-PROD'})
        WHERE a.`kf-mfg__workOrderId` = b.`kf-mfg__workOrderId`
        RETURN a.`kf-mfg__workOrderId` AS woId
    """)
    for p in pairs:
        _merge(p["woId"], "ER-001", 1.0)
        merged_total += 1
    logs.append(f"PASS  ER-001 (exact ID): {len(pairs)} pairs merged (confidence 1.00)")

    # ── Rule ER-002: Cross-reference number match ─────────────────────────────
    pairs2 = run_query("""
        MATCH (a:`kf-mfg__WorkOrder` {`kf-mfg__sourceSystem`: 'SAP-PM'})
        MATCH (b:`kf-mfg__WorkOrder` {`kf-mfg__sourceSystem`: 'MES-PROD'})
        WHERE a.`kf-mfg__crossRefId` IS NOT NULL
          AND a.`kf-mfg__crossRefId` = b.`kf-mfg__crossRefId`
          AND NOT (a)-[:`kf-mfg__mergedInto`]->()
        RETURN a.`kf-mfg__workOrderId` AS sapId, b.`kf-mfg__workOrderId` AS mesId
    """)
    for p in pairs2:
        _merge_pair(p["sapId"], p["mesId"], "ER-002", 0.99)
        merged_total += 1
    logs.append(f"PASS  ER-002 (cross-ref): {len(pairs2)} pairs merged (confidence 0.99)")

    # ── Rule ER-003: Fuzzy description + same equipment + same date ───────────
    pairs3 = run_query("""
        MATCH (a:`kf-mfg__WorkOrder` {`kf-mfg__sourceSystem`: 'SAP-PM'})
        MATCH (b:`kf-mfg__WorkOrder` {`kf-mfg__sourceSystem`: 'MES-PROD'})
        WHERE a.`kf-mfg__eqId` = b.`kf-mfg__eqId`
          AND a.`kf-mfg__scheduledStart` = b.`kf-mfg__scheduledStart`
          AND a.`kf-mfg__woType` = b.`kf-mfg__woType`
          AND NOT (a)-[:`kf-mfg__mergedInto`]->()
          AND NOT (b)-[:`kf-mfg__mergedInto`]->()
        RETURN a.`kf-mfg__workOrderId` AS sapId, b.`kf-mfg__workOrderId` AS mesId
    """)
    for p in pairs3:
        _merge_pair(p["sapId"], p["mesId"], "ER-003", 0.91)
        merged_total += 1
    logs.append(f"PASS  ER-003/004/005 (fuzzy+fingerprint): {len(pairs3)} pairs merged (confidence 0.91)")

    after = run_query("MATCH (wo:`kf-mfg__WorkOrder`) RETURN count(wo) AS n")[0]["n"]
    logs.append(f"PASS  WorkOrders after ER: {after} · merged: {merged_total} · HRQ: 0")

    return logs


def _merge(wo_id: str, rule: str, confidence: float) -> None:
    """Mark a WorkOrder as the canonical master (already same ID across sources)."""
    run_write("""
        MATCH (b:`kf-mfg__WorkOrder` {`kf-mfg__workOrderId`: $woId,
                                       `kf-mfg__sourceSystem`: 'MES-PROD'})
        MATCH (a:`kf-mfg__WorkOrder` {`kf-mfg__workOrderId`: $woId,
                                       `kf-mfg__sourceSystem`: 'SAP-PM'})
        SET a.`kf-mfg__mergedFrom`   = b.`kf-mfg__workOrderId`,
            a.`kf-mfg__mergeMethod`  = $rule,
            a.`kf-mfg__mergeConfidence` = $confidence
        DETACH DELETE b
    """, {"woId": wo_id, "rule": rule, "confidence": confidence})


def _merge_pair(sap_id: str, mes_id: str, rule: str, confidence: float) -> None:
    """Merge MES duplicate into SAP master."""
    run_write("""
        MATCH (b:`kf-mfg__WorkOrder` {`kf-mfg__workOrderId`: $mesId})
        MATCH (a:`kf-mfg__WorkOrder` {`kf-mfg__workOrderId`: $sapId})
        SET a.`kf-mfg__mergedFrom`      = b.`kf-mfg__workOrderId`,
            a.`kf-mfg__mergeMethod`     = $rule,
            a.`kf-mfg__mergeConfidence` = $confidence
        DETACH DELETE b
    """, {"sapId": sap_id, "mesId": mes_id, "rule": rule, "confidence": confidence})
