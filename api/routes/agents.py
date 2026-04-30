from fastapi import APIRouter, HTTPException
from agents import AGENT_REGISTRY
from api.schemas import AgentRunRequest, AgentRunResponse

router = APIRouter()


@router.get("")
def list_agents():
    """List available agents."""
    return {"agents": list(AGENT_REGISTRY.keys())}


@router.post("/run", response_model=AgentRunResponse)
def run_agent_endpoint(req: AgentRunRequest):
    """Run a named agent against the knowledge graph."""
    if req.agent not in AGENT_REGISTRY:
        raise HTTPException(
            status_code=404,
            detail=f"Agent '{req.agent}' not found. Available: {list(AGENT_REGISTRY.keys())}",
        )
    fn = AGENT_REGISTRY[req.agent]
    result = fn()
    return AgentRunResponse(agent=req.agent, result=result)
