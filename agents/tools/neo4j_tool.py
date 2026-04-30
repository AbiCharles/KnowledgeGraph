"""LangChain tool that executes Cypher queries against the knowledge graph."""

from langchain_core.tools import tool
from db import run_query


@tool
def cypher_query(query: str) -> str:
    """
    Execute a read-only Cypher query against the KF WorkOrder Knowledge Graph
    and return the results as a formatted string.

    The graph contains these node labels (all prefixed with kf-mfg__):
    - WorkOrder   (workOrderId, woType, woStatus, woPriority, sourceSystem,
                   scheduledStart, description, eqId, techId, policyId,
                   mergedFrom, mergeMethod)
    - Equipment   (equipmentId, name, tag, location, status, lastPM)
    - ProductionLine (lineId, name, plant, capacity, status)
    - CompliancePolicy (policyId, name, regBody, standard, mandatory)
    - Technician  (technicianId, name, grade, cert, specialisation)
    - IngestionAdapter (adapterId, sourceSystem, protocol, syncMode)

    Relationship types:
    - assignedToEquipment   WorkOrder -> Equipment
    - onProductionLine      Equipment -> ProductionLine
    - assignedToTechnician  WorkOrder -> Technician
    - governedBy            WorkOrder -> CompliancePolicy
    - sourcedFrom           WorkOrder -> IngestionAdapter

    All property names use the kf-mfg__ prefix, e.g.:
        MATCH (wo:`kf-mfg__WorkOrder`)
        WHERE wo.`kf-mfg__woStatus` = 'OPEN'
        RETURN wo.`kf-mfg__workOrderId` AS id,
               wo.`kf-mfg__woPriority` AS priority

    Always use backtick-quoted labels and properties.
    Only generate read queries (MATCH, RETURN, WITH, WHERE, ORDER BY, LIMIT).
    """
    try:
        rows = run_query(query)
        if not rows:
            return "Query returned no results."
        # Format as a readable table string
        cols = list(rows[0].keys())
        lines = [" | ".join(cols)]
        lines.append("-" * len(lines[0]))
        for row in rows:
            lines.append(" | ".join(str(row.get(c, "")) for c in cols))
        return "\n".join(lines)
    except Exception as exc:
        return f"Query error: {exc}"
