from fastapi import APIRouter

from pipeline.ontology_curation import run_curation
from pipeline.run import StageResult
from api.schemas import PipelineRunResponse, StageResultSchema

router = APIRouter()


@router.post("/curate", response_model=PipelineRunResponse)
def curate_ontology():
    """Run the 6-step ontology curation against ontology/kf-mfg-workorder.ttl.

    Reuses the PipelineRunResponse shape so the frontend can render curation
    steps with the same component as hydration stages.
    """
    completed: list[StageResultSchema] = []
    overall = "pass"

    for result in run_curation():
        if result.status in ("pass", "fail"):
            completed.append(StageResultSchema(**vars(result)))
        if result.status == "fail":
            overall = "fail"
            break

    return PipelineRunResponse(stages=completed, overall=overall)
