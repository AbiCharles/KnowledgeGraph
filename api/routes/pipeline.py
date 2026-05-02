import json

from fastapi import APIRouter, HTTPException, Request
from fastapi.responses import StreamingResponse

from pipeline.run import run_pipeline
from pipeline import use_case_registry
from pipeline.schema_introspection import invalidate_schema_cache
from api import locks
from api.locks import acquire_or_409
from api.schemas import PipelineRunResponse, StageResultSchema

router = APIRouter()


@router.post("/run", response_model=PipelineRunResponse)
async def run_pipeline_endpoint(request: Request):
    """Run the 7-stage hydration pipeline against the active use case.

    Default is a blocking JSON response — suitable for tests + regression
    scripts. Add `?stream=true` and read the response as Server-Sent Events
    (each event is `data: <json>\\n\\n`) to receive stages as they finish.

    Both modes share the same lock so concurrent calls return 409.
    """
    streaming = request.query_params.get("stream", "").lower() in ("1", "true", "yes")

    # 409 fast-fail before kicking off either response shape.
    if locks.pipeline_lock.locked():
        raise HTTPException(status_code=409, detail="A pipeline run is already in progress.")

    try:
        use_case = use_case_registry.get_active()
    except RuntimeError as exc:
        raise HTTPException(status_code=404, detail=str(exc))

    if streaming:
        return StreamingResponse(
            _stream_pipeline(use_case),
            media_type="text/event-stream",
            headers={"Cache-Control": "no-cache", "X-Accel-Buffering": "no"},
        )

    async with acquire_or_409(locks.pipeline_lock, "pipeline"):
        completed: list[StageResultSchema] = []
        overall = "pass"
        for result in run_pipeline(use_case):
            if result.status in ("pass", "fail"):
                completed.append(StageResultSchema(**vars(result)))
            if result.status == "fail":
                overall = "fail"
                break
        invalidate_schema_cache()
        return PipelineRunResponse(stages=completed, overall=overall)


async def _stream_pipeline(use_case):
    """Server-Sent Events generator. Acquires the pipeline lock for the full
    run; yields one event per finished stage plus a final summary event."""
    async with acquire_or_409(locks.pipeline_lock, "pipeline"):
        completed = []
        overall = "pass"
        for result in run_pipeline(use_case):
            if result.status not in ("pass", "fail"):
                continue
            stage_payload = StageResultSchema(**vars(result)).model_dump()
            completed.append(stage_payload)
            yield "event: stage\ndata: " + json.dumps(stage_payload) + "\n\n"
            if result.status == "fail":
                overall = "fail"
                break
        invalidate_schema_cache()
        yield "event: done\ndata: " + json.dumps({"overall": overall, "count": len(completed)}) + "\n\n"
