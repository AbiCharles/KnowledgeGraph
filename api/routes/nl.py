"""Natural-language to Cypher endpoint.

Used as a fallback in the frontend's plain-English query mode when the
client-side regex rules don't recognise a question. The schema portion of the
prompt comes from `pipeline.schema_introspection.schema_description()`,
which is cached centrally on (slug, ontology_mtime, data_mtime) so the same
introspection is shared with agents/dynamic.py.
"""
from __future__ import annotations
import json
import logging
import re

from fastapi import APIRouter, HTTPException
from langchain_openai import ChatOpenAI

from config import get_settings
from pipeline import use_case_registry
from pipeline.schema_introspection import schema_description
from pipeline.cypher_safety import assert_read_only, UnsafeCypherError
from api.llm_usage import (
    assert_within_daily_cap, record_call, extract_token_counts,
)
from api.schemas import NLRequest, NLResponse


router = APIRouter()
log = logging.getLogger(__name__)


def _active_schema_prompt() -> str:
    uc = use_case_registry.get_active()
    schema = schema_description(uc)
    return (
        f"You translate plain-English questions about the '{uc.manifest.name}' knowledge graph "
        f"into read-only Cypher queries.\n\n"
        f"{schema}\n"
        f"- Default LIMIT 50 unless the question explicitly asks for a count or all rows.\n\n"
        f"Respond in strict JSON with two keys:\n"
        f'{{"cypher": "<cypher>", "explanation": "<one short sentence>"}}'
    )


@router.post("", response_model=NLResponse)
def nl_to_cypher(req: NLRequest):
    s = get_settings()
    assert_within_daily_cap()
    try:
        system_prompt = _active_schema_prompt()
    except RuntimeError as exc:
        raise HTTPException(status_code=404, detail=str(exc))

    llm = ChatOpenAI(
        model=s.openai_model,
        api_key=s.openai_api_key,
        temperature=0,
        model_kwargs={"response_format": {"type": "json_object"}},
    )

    try:
        response = llm.invoke([
            ("system", system_prompt),
            ("user", req.question),
        ])
    except Exception as exc:
        log.warning("LLM invocation failed: %s", exc)
        raise HTTPException(status_code=502, detail=f"LLM call failed: {exc}")

    in_t, out_t = extract_token_counts(response)
    record_call(s.openai_model, in_t, out_t, kind="nl")

    try:
        payload = json.loads(response.content)
        cypher = payload["cypher"].strip()
        explanation = payload.get("explanation", "").strip()
    except (json.JSONDecodeError, KeyError) as exc:
        raise HTTPException(status_code=502, detail=f"Model did not return valid JSON: {exc}")

    cypher_clean = re.sub(r"^```(?:cypher)?\s*|\s*```$", "", cypher, flags=re.IGNORECASE).strip()
    try:
        assert_read_only(cypher_clean, source="LLM-generated cypher")
    except UnsafeCypherError as exc:
        raise HTTPException(status_code=400, detail=str(exc))

    return NLResponse(cypher=cypher_clean, explanation=explanation)
