"""Stage 6 — Validation: run 4 critical + 4 warning checks on the finished graph."""

from db import run_query


def validate(ctx: dict) -> list[str]:
    logs = []
    failures = []

    # ── Critical checks (fail pipeline if not met) ────────────────────────────
    wo_count = run_query(
        "MATCH (wo:`kf-mfg__WorkOrder`) RETURN count(wo) AS n"
    )[0]["n"]
    if wo_count < 20:
        failures.append(f"VC-C1 WorkOrder count {wo_count} < 20")
    else:
        logs.append(f"PASS  VC-C1 WorkOrder count: {wo_count} >= 20")

    eq_count = run_query(
        "MATCH (eq:`kf-mfg__Equipment`) RETURN count(eq) AS n"
    )[0]["n"]
    if eq_count != 5:
        failures.append(f"VC-C2 Equipment count {eq_count} != 5")
    else:
        logs.append(f"PASS  VC-C2 Equipment count: {eq_count} = 5")

    dup = run_query("""
        MATCH (wo:`kf-mfg__WorkOrder`)
        WITH wo.`kf-mfg__workOrderId` AS id, count(*) AS cnt
        WHERE cnt > 1
        RETURN count(*) AS dups
    """)[0]["dups"]
    if dup > 0:
        failures.append(f"VC-C3 {dup} duplicate workOrderIds found")
    else:
        logs.append("PASS  VC-C3 No duplicate workOrderIds")

    orphans = run_query("""
        MATCH (wo:`kf-mfg__WorkOrder`)
        WHERE NOT (wo)--()
        RETURN count(wo) AS n
    """)[0]["n"]
    if orphans > 0:
        failures.append(f"VC-C4 {orphans} orphaned WorkOrder nodes")
    else:
        logs.append("PASS  VC-C4 No orphaned WorkOrder nodes")

    if failures:
        raise RuntimeError("Validation failed: " + "; ".join(failures))

    # ── Warning checks (log only) ─────────────────────────────────────────────
    no_prov = run_query("""
        MATCH (wo:`kf-mfg__WorkOrder`)
        WHERE wo.`kf-mfg__sourceSystem` IS NULL
        RETURN count(wo) AS n
    """)[0]["n"]
    logs.append(f"{'WARN' if no_prov else 'PASS'}  VC-W1 Provenance gaps: {no_prov}")

    hrq = run_query("""
        MATCH (wo:`kf-mfg__WorkOrder`)
        WHERE wo.`kf-mfg__hrqPending` = true
        RETURN count(wo) AS n
    """)[0]["n"]
    logs.append(f"{'WARN' if hrq else 'PASS'}  VC-W2 HRQ queue: {hrq} pending")

    unsourced = run_query("""
        MATCH (wo:`kf-mfg__WorkOrder`)
        WHERE NOT (wo)-[:`kf-mfg__sourcedFrom`]->()
        RETURN count(wo) AS n
    """)[0]["n"]
    logs.append(f"{'WARN' if unsourced else 'PASS'}  VC-W3 Unsourced WorkOrders: {unsourced}")

    reachable = run_query("""
        MATCH path = (wo:`kf-mfg__WorkOrder`)
              -[:`kf-mfg__assignedToEquipment`]->
              (:`kf-mfg__Equipment`)
              -[:`kf-mfg__onProductionLine`]->
              (:`kf-mfg__ProductionLine`)
        RETURN count(path) AS n
    """)[0]["n"]
    logs.append(f"PASS  VC-W4 3-hop connectivity: {reachable} paths")

    return logs
