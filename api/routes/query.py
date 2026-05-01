from fastapi import APIRouter, HTTPException
from db import run_query
from api.schemas import QueryRequest, QueryResponse
from pipeline.cypher_safety import assert_read_only, UnsafeCypherError

router = APIRouter()


@router.post("", response_model=QueryResponse)
def execute_query(req: QueryRequest):
    """Execute a read-only Cypher query against the knowledge graph."""
    try:
        assert_read_only(req.cypher, source="user query")
    except UnsafeCypherError as exc:
        raise HTTPException(status_code=400, detail=str(exc))

    try:
        rows = run_query(req.cypher)
    except Exception as exc:
        raise HTTPException(status_code=422, detail=str(exc))

    if not rows:
        return QueryResponse(columns=[], rows=[], row_count=0)

    cols = list(rows[0].keys())
    return QueryResponse(columns=cols, rows=rows, row_count=len(rows))
