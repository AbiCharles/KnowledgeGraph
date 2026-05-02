import logging

from fastapi import APIRouter, HTTPException

from agents import dynamic
from pipeline import use_case_registry
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
    try:
        result = dynamic.run(use_case, req.agent)
    except Exception as exc:
        log.exception("Agent %s failed", req.agent)
        raise HTTPException(status_code=502, detail=f"Agent execution failed: {exc}")
    return AgentRunResponse(agent=req.agent, result=result)
