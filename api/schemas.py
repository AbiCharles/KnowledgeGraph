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


class NLRequest(BaseModel):
    question: str


class NLResponse(BaseModel):
    cypher: str
    explanation: str


class SetActiveRequest(BaseModel):
    slug: str


class UseCaseSummary(BaseModel):
    slug: str
    name: str
    description: str = ""
    prefix: str
    namespace: str
    in_scope_classes: list[str] = []
    agent_count: int = 0
    is_active: bool = False


class UseCaseListResponse(BaseModel):
    active: str | None
    bundles: list[UseCaseSummary]
