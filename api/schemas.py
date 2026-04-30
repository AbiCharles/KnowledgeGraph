from pydantic import BaseModel
from typing import Any


class StageResultSchema(BaseModel):
    stage: int
    name: str
    status: str
    logs: list[str] = []
    duration_ms: int = 0
    error: str | None = None


class PipelineRunResponse(BaseModel):
    stages: list[StageResultSchema]
    overall: str   # "pass" | "fail"


class QueryRequest(BaseModel):
    cypher: str


class QueryResponse(BaseModel):
    columns: list[str]
    rows: list[dict[str, Any]]
    row_count: int


class AgentRunRequest(BaseModel):
    agent: str   # "maintenance_planner" | "compliance_monitor" | "root_cause_analyst"


class AgentRunResponse(BaseModel):
    agent: str
    result: str
