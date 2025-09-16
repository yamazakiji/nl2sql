from __future__ import annotations

from datetime import datetime
from typing import Any, Literal, Optional

from pydantic import BaseModel, Field


class ProjectCreate(BaseModel):
    name: str


class ProjectResponse(BaseModel):
    id: str
    name: str
    created_at: datetime

    model_config = {"from_attributes": True}


class ConnectorCreate(BaseModel):
    type: Literal["sqlite", "postgres", "duckdb"]
    name: str
    dsn: str


class ConnectorResponse(BaseModel):
    id: str
    type: str
    name: str
    created_at: datetime
    dsn_masked: str


class SchemaSnapshotCreate(BaseModel):
    pass


class SchemaSnapshotResponse(BaseModel):
    id: str
    connector_id: str
    status: str
    job_id: Optional[str]
    artifact_path: Optional[str]
    created_at: datetime

    model_config = {"from_attributes": True}


class TrainingCreate(BaseModel):
    project: str
    schema_snapshot: str
    config_ref: str


class TrainingResponse(BaseModel):
    id: str
    project_id: str
    schema_snapshot_id: str
    status: str
    config_path: str
    job_id: Optional[str]
    metrics: Optional[dict[str, Any]]
    created_at: datetime

    model_config = {"from_attributes": True}


class DeploymentCreate(BaseModel):
    run: str
    label: str


class DeploymentResponse(BaseModel):
    id: str
    project_id: str
    run_id: str
    label: str
    status: str
    created_at: datetime

    model_config = {"from_attributes": True}


class PlanRequest(BaseModel):
    question: str
    deployment: str
    connector: str


class PlanCandidate(BaseModel):
    sql: str
    rationale: str
    explain_summary: str
    est_cost: float


class PlanResponse(BaseModel):
    run_id: str
    candidates: list[PlanCandidate]
    clarifications: list[str]


class ExecuteRequest(BaseModel):
    run_id: str
    approved_sql: str
    connector: str
    limit: int = 100


class ExecuteResponse(BaseModel):
    run_id: str
    row_count: int
    rows: list[dict[str, Any]]
    result_ref: str


class ChatMessage(BaseModel):
    role: Literal["user", "assistant"]
    content: str


class ChatRequest(BaseModel):
    deployment: str
    connector: str
    history: list[ChatMessage] = Field(default_factory=list)


class ChatResponse(BaseModel):
    run_id: str
    messages: list[ChatMessage]


class ConnectorTestResponse(BaseModel):
    status: str
    details: str


class MetricsResponse(BaseModel):
    requests_total: int
    successful_requests: int
    failed_requests: int


class InferenceRunSummary(BaseModel):
    id: str
    question: str
    status: str
    created_at: datetime

    model_config = {"from_attributes": True}


class InferenceRunList(BaseModel):
    items: list[InferenceRunSummary]
