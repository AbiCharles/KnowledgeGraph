import logging

from fastapi import APIRouter, HTTPException

from agents import dynamic
from pipeline import use_case_registry
from api.llm_usage import assert_within_daily_cap, record_call
from config import get_settings
from api.schemas import AgentRunRequest, AgentRunResponse

router = APIRouter()
log = logging.getLogger(__name__)


@router.get("")
def list_agents():
    """List agents declared by the active use case manifest."""
    try:
        use_case = use_case_registry.get_active()
    except RuntimeError as exc:
        raise HTTPException(status_code=404, detail=str(exc))
    return {"agents": dynamic.list_agents(use_case)}


@router.post("/run", response_model=AgentRunResponse)
def run_agent_endpoint(req: AgentRunRequest):
    """Run a named agent (declared in the active manifest) against the knowledge graph."""
    try:
        use_case = use_case_registry.get_active()
    except RuntimeError as exc:
        raise HTTPException(status_code=404, detail=str(exc))
    if dynamic.find_agent(use_case, req.agent) is None:
        available = [a["id"] for a in dynamic.list_agents(use_case)]
        raise HTTPException(
            status_code=404,
            detail=f"Agent '{req.agent}' not found in use case '{use_case.slug}'. Available: {available}",
        )
    assert_within_daily_cap()
    try:
        result = dynamic.run(use_case, req.agent)
    except Exception as exc:
        log.exception("Agent %s failed", req.agent)
        raise HTTPException(status_code=502, detail=f"Agent execution failed: {exc}")
    # LangGraph hides per-call token usage; estimate from the visible
    # input (system_prompt + task) and output (final response). The agent's
    # internal tool-calling round-trips inflate this somewhat, so we apply
    # a 3x multiplier as a rough multi-step approximation.
    s = get_settings()
    spec = dynamic.find_agent(use_case, req.agent)
    in_chars = len(spec.system_prompt or "") + len(spec.task or "")
    out_chars = len(result or "")
    # Rough heuristic: ~4 chars per token, then 3x for ReAct round-trips.
    record_call(
        s.openai_model,
        input_tokens=int(in_chars / 4 * 3),
        output_tokens=int(out_chars / 4),
        kind="agent",
    )
    return AgentRunResponse(agent=req.agent, result=result)
