from fastapi import APIRouter
from pipeline.run import run_pipeline, StageResult
from api.schemas import PipelineRunResponse, StageResultSchema

router = APIRouter()


@router.post("/run", response_model=PipelineRunResponse)
def run_pipeline_endpoint():
    """Run the full 7-stage hydration pipeline and return all stage results."""
    completed: list[StageResultSchema] = []
    overall = "pass"

    for result in run_pipeline():
        # Only record final state (pass/fail), not the intermediate "running" state
        if result.status in ("pass", "fail"):
            completed.append(StageResultSchema(**vars(result)))
        if result.status == "fail":
            overall = "fail"
            break

    return PipelineRunResponse(stages=completed, overall=overall)
