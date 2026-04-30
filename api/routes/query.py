from fastapi import APIRouter, HTTPException
from db import run_query
from api.schemas import QueryRequest, QueryResponse

router = APIRouter()


@router.post("", response_model=QueryResponse)
def execute_query(req: QueryRequest):
    """Execute a read-only Cypher query against the knowledge graph."""
    # Basic safety: block write keywords
    banned = ("create", "merge", "delete", "set", "remove", "drop")
    if any(kw in req.cypher.lower() for kw in banned):
        raise HTTPException(status_code=400, detail="Write operations are not allowed via this endpoint.")

    try:
        rows = run_query(req.cypher)
    except Exception as exc:
        raise HTTPException(status_code=422, detail=str(exc))

    if not rows:
        return QueryResponse(columns=[], rows=[], row_count=0)

    cols = list(rows[0].keys())
    return QueryResponse(columns=cols, rows=rows, row_count=len(rows))
