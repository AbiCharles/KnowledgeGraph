from fastapi import APIRouter, HTTPException

from agents import dynamic
from pipeline import use_case_registry
from api.schemas import AgentRunRequest, AgentRunResponse

router = APIRouter()


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
    result = dynamic.run(use_case, req.agent)
    return AgentRunResponse(agent=req.agent, result=result)
