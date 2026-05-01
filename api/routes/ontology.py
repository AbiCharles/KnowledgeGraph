from fastapi import APIRouter, HTTPException

from pipeline.ontology_curation import run_curation
from pipeline import use_case_registry
from api.locks import curation_lock
from api.schemas import PipelineRunResponse, StageResultSchema

router = APIRouter()


@router.post("/curate", response_model=PipelineRunResponse)
async def curate_ontology():
    """Run the 6-step ontology curation against the active use case's TTL."""
    if curation_lock.locked():
        raise HTTPException(status_code=409, detail="A curation run is already in progress.")
    async with curation_lock:
        try:
            use_case = use_case_registry.get_active()
        except RuntimeError as exc:
            raise HTTPException(status_code=404, detail=str(exc))

        completed: list[StageResultSchema] = []
        overall = "pass"

        for result in run_curation(use_case):
            if result.status in ("pass", "fail"):
                completed.append(StageResultSchema(**vars(result)))
            if result.status == "fail":
                overall = "fail"
                break

        return PipelineRunResponse(stages=completed, overall=overall)
