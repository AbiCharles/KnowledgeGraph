from fastapi import APIRouter, HTTPException

from pipeline.run import run_pipeline
from pipeline import use_case_registry
from api.schemas import PipelineRunResponse, StageResultSchema

router = APIRouter()


@router.post("/run", response_model=PipelineRunResponse)
def run_pipeline_endpoint():
    """Run the 7-stage hydration pipeline against the active use case."""
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

    return PipelineRunResponse(stages=completed, overall=overall)
