"""Natural-language to Cypher endpoint.

Used as a fallback in the frontend's plain-English query mode when the
client-side regex rules don't recognise a question.
"""
from __future__ import annotations
import json
import re

from fastapi import APIRouter, HTTPException
from langchain_openai import ChatOpenAI

from config import get_settings
from api.schemas import NLRequest, NLResponse


SCHEMA_PROMPT = """You translate plain-English questions about a manufacturing
maintenance knowledge graph into read-only Cypher queries.

Graph schema (all labels and properties are prefixed with kf-mfg__):

Node labels:
- WorkOrder        (workOrderId, woType, woStatus, woPriority, sourceSystem,
                    scheduledStart, description, eqId, techId, policyId,
                    crossRefId, mergedFrom, mergeMethod, mergeConfidence)
- Equipment        (equipmentId, name, tag, location, status, lastPM)
- ProductionLine   (lineId, name, plant, capacity, status)
- CompliancePolicy (policyId, name, regBody, standard, mandatory, reviewCycle)
- Technician       (technicianId, name, grade, cert, specialisation)
- IngestionAdapter (adapterId, sourceSystem, protocol, syncMode, registered)

Relationship types (ALL prefixed with kf-mfg__ in Neo4j; always backtick-quote them):
- `kf-mfg__assignedToEquipment`    WorkOrder -> Equipment
- `kf-mfg__onProductionLine`       Equipment -> ProductionLine
- `kf-mfg__assignedToTechnician`   WorkOrder -> Technician
- `kf-mfg__governedBy`             WorkOrder -> CompliancePolicy
- `kf-mfg__sourcedFrom`            WorkOrder -> IngestionAdapter

Cypher rules:
- Always backtick-quote labels, properties, AND relationship types: `kf-mfg__WorkOrder`, `kf-mfg__woStatus`, `kf-mfg__assignedToEquipment`.
- IMPORTANT: relationship types MUST include the `kf-mfg__` prefix — write `-[:\`kf-mfg__assignedToEquipment\`]->` not `-[:\`assignedToEquipment\`]->`.
- Always alias RETURN columns with AS: `RETURN wo.\`kf-mfg__workOrderId\` AS id`.
- Read-only only: MATCH, OPTIONAL MATCH, WITH, WHERE, RETURN, ORDER BY, LIMIT.
- Never use CREATE, MERGE, DELETE, SET, REMOVE, DROP, LOAD, CALL.
- Default LIMIT 50 unless the question explicitly asks for a count or all rows.

Respond in strict JSON with two keys:
{
  "cypher": "<the cypher query, no surrounding backticks or 'cypher' fence>",
  "explanation": "<one short sentence describing what the query returns>"
}
"""

WRITE_KEYWORDS = ("create ", "merge ", "delete ", "set ", "remove ", "drop ", "load ", "call ")

router = APIRouter()


@router.post("", response_model=NLResponse)
def nl_to_cypher(req: NLRequest):
    s = get_settings()
    llm = ChatOpenAI(
        model=s.openai_model,
        api_key=s.openai_api_key,
        temperature=0,
        model_kwargs={"response_format": {"type": "json_object"}},
    )

    response = llm.invoke([
        ("system", SCHEMA_PROMPT),
        ("user", req.question),
    ])

    try:
        payload = json.loads(response.content)
        cypher = payload["cypher"].strip()
        explanation = payload.get("explanation", "").strip()
    except (json.JSONDecodeError, KeyError) as exc:
        raise HTTPException(status_code=502, detail=f"Model did not return valid JSON: {exc}")

    cypher_clean = re.sub(r"^```(?:cypher)?\s*|\s*```$", "", cypher, flags=re.IGNORECASE).strip()
    lowered = cypher_clean.lower()
    if any(kw in lowered for kw in WRITE_KEYWORDS):
        raise HTTPException(status_code=400, detail="Generated query contains a write operation; rejected.")

    return NLResponse(cypher=cypher_clean, explanation=explanation)
