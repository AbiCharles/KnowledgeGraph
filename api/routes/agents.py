import logging

from fastapi import APIRouter, HTTPException

from agents import dynamic, memory
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
    s = get_settings()
    spec = dynamic.find_agent(use_case, req.agent)

    # Persist the conversation as a :Conversation/:Message subgraph in the
    # active bundle's database. Memory writes are best-effort — a Neo4j
    # hiccup must not fail the agent run that already produced a result.
    cid = None
    try:
        cid = memory.start_conversation(use_case.slug, req.agent, spec.name, model=s.openai_model)
        memory.record_message(cid, "system", spec.system_prompt or "")
        memory.record_message(cid, "user", spec.task or "")
    except Exception as exc:
        log.warning("Could not persist agent conversation start: %s", exc)
        cid = None

    try:
        result = dynamic.run(use_case, req.agent)
    except Exception as exc:
        if cid:
            try: memory.end_conversation(cid, status="failed")
            except Exception: pass
        log.exception("Agent %s failed", req.agent)
        raise HTTPException(status_code=502, detail=f"Agent execution failed: {exc}")

    if cid:
        try:
            memory.record_message(cid, "assistant", result or "")
            memory.end_conversation(cid, status="completed")
        except Exception as exc:
            log.warning("Could not persist agent conversation end: %s", exc)

    # LangGraph hides per-call token usage; estimate from the visible
    # input (system_prompt + task) and output (final response). The agent's
    # internal tool-calling round-trips inflate this somewhat, so we apply
    # a 3x multiplier as a rough multi-step approximation.
    in_chars = len(spec.system_prompt or "") + len(spec.task or "")
    out_chars = len(result or "")
    record_call(
        s.openai_model,
        input_tokens=int(in_chars / 4 * 3),
        output_tokens=int(out_chars / 4),
        kind="agent",
    )
    return AgentRunResponse(agent=req.agent, result=result, conversation_id=cid)


@router.get("/conversations")
def list_agent_conversations(agent_id: str | None = None, limit: int = 20):
    """Recent agent runs in the active bundle's database, newest first.
    Filter to one agent with `?agent_id=`."""
    try:
        use_case = use_case_registry.get_active()
    except RuntimeError as exc:
        raise HTTPException(status_code=404, detail=str(exc))
    try:
        rows = memory.list_conversations(slug=use_case.slug, agent_id=agent_id, limit=limit)
    except Exception as exc:
        raise HTTPException(status_code=503, detail=f"Conversation history unavailable: {exc}")
    return {"slug": use_case.slug, "conversations": rows}


@router.get("/conversations/{cid}")
def get_agent_conversation(cid: str):
    try:
        out = memory.get_conversation(cid)
    except Exception as exc:
        raise HTTPException(status_code=503, detail=f"Could not load conversation: {exc}")
    if out is None:
        raise HTTPException(status_code=404, detail=f"No conversation {cid!r}")
    return out


@router.delete("/conversations/{cid}")
def delete_agent_conversation(cid: str):
    try:
        memory.delete_conversation(cid)
    except Exception as exc:
        raise HTTPException(status_code=503, detail=f"Could not delete conversation: {exc}")
    return {"deleted": cid}
