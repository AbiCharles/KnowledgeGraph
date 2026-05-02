from fastapi import APIRouter, HTTPException

from pipeline.run import run_pipeline
from pipeline import use_case_registry
from pipeline.schema_introspection import invalidate_schema_cache
from api import locks
from api.locks import acquire_or_409
from api.schemas import PipelineRunResponse, StageResultSchema

router = APIRouter()


@router.post("/run", response_model=PipelineRunResponse)
async def run_pipeline_endpoint():
    """Run the 7-stage hydration pipeline against the active use case.

    Concurrent calls return 409 immediately (non-blocking acquire — no
    TOCTOU race between checking and acquiring the lock).
    """
    async with acquire_or_409(locks.pipeline_lock, "pipeline"):
        try:
            use_case = use_case_registry.get_active()
        except RuntimeError as exc:
            raise HTTPException(status_code=404, detail=str(exc))

        completed: list[StageResultSchema] = []
        overall = "pass"

        for result in run_pipeline(use_case):
            if result.status in ("pass", "fail"):
                completed.append(StageResultSchema(**vars(result)))
            if result.status == "fail":
                overall = "fail"
                break

        # Pipeline rewrites Neo4j data so any cached enum samples are stale.
        invalidate_schema_cache()
        return PipelineRunResponse(stages=completed, overall=overall)
