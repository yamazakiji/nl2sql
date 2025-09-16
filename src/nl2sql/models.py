from __future__ import annotations

import uuid
from datetime import UTC, datetime
from typing import Optional

from enum import Enum

from sqlalchemy import ForeignKey, String, Text
from sqlalchemy.dialects.sqlite import JSON
from sqlalchemy.orm import Mapped, mapped_column, relationship

from nl2sql.db import Base


def utcnow() -> datetime:
    return datetime.now(UTC)


def _uuid() -> str:
    return str(uuid.uuid4())


class Project(Base):
    __tablename__ = "projects"

    id: Mapped[str] = mapped_column(String, primary_key=True, default=_uuid)
    name: Mapped[str] = mapped_column(String, unique=True)
    created_at: Mapped[datetime] = mapped_column(default=utcnow)

    deployments: Mapped[list[Deployment]] = relationship("Deployment", back_populates="project")


class Connector(Base):
    __tablename__ = "connectors"

    id: Mapped[str] = mapped_column(String, primary_key=True, default=_uuid)
    name: Mapped[str] = mapped_column(String)
    type: Mapped[str] = mapped_column(String)
    dsn: Mapped[str] = mapped_column(Text)
    created_at: Mapped[datetime] = mapped_column(default=utcnow)

    schema_snapshots: Mapped[list[SchemaSnapshot]] = relationship(
        "SchemaSnapshot", back_populates="connector"
    )


class SchemaSnapshotStatus(str, Enum):
    queued = "queued"
    running = "running"
    completed = "completed"
    failed = "failed"


class SchemaSnapshot(Base):
    __tablename__ = "schema_snapshots"

    id: Mapped[str] = mapped_column(String, primary_key=True, default=_uuid)
    connector_id: Mapped[str] = mapped_column(ForeignKey("connectors.id"))
    status: Mapped[str] = mapped_column(String, default=SchemaSnapshotStatus.queued.value)
    artifact_path: Mapped[Optional[str]] = mapped_column(String, nullable=True)
    created_at: Mapped[datetime] = mapped_column(default=utcnow)
    job_id: Mapped[Optional[str]] = mapped_column(String, nullable=True)

    connector: Mapped[Connector] = relationship("Connector", back_populates="schema_snapshots")


class TrainingStatus(str, Enum):
    queued = "queued"
    running = "running"
    completed = "completed"
    failed = "failed"


class TrainingRun(Base):
    __tablename__ = "training_runs"

    id: Mapped[str] = mapped_column(String, primary_key=True, default=_uuid)
    project_id: Mapped[str] = mapped_column(ForeignKey("projects.id"))
    schema_snapshot_id: Mapped[str] = mapped_column(ForeignKey("schema_snapshots.id"))
    status: Mapped[str] = mapped_column(String, default=TrainingStatus.queued.value)
    config_path: Mapped[str] = mapped_column(String)
    created_at: Mapped[datetime] = mapped_column(default=utcnow)
    job_id: Mapped[Optional[str]] = mapped_column(String, nullable=True)
    metrics: Mapped[dict[str, object] | None] = mapped_column(JSON, default=None)

    project: Mapped[Project] = relationship("Project")
    schema_snapshot: Mapped[SchemaSnapshot] = relationship("SchemaSnapshot")


class DeploymentStatus(str, Enum):
    active = "active"
    inactive = "inactive"


class Deployment(Base):
    __tablename__ = "deployments"

    id: Mapped[str] = mapped_column(String, primary_key=True, default=_uuid)
    project_id: Mapped[str] = mapped_column(ForeignKey("projects.id"))
    run_id: Mapped[str] = mapped_column(ForeignKey("training_runs.id"))
    label: Mapped[str] = mapped_column(String, unique=True)
    status: Mapped[str] = mapped_column(String, default=DeploymentStatus.active.value)
    created_at: Mapped[datetime] = mapped_column(default=utcnow)

    project: Mapped[Project] = relationship("Project", back_populates="deployments")
    training_run: Mapped[TrainingRun] = relationship("TrainingRun")


class RunStatus(str, Enum):
    planning = "planning"
    awaiting_approval = "awaiting_approval"
    executing = "executing"
    completed = "completed"
    failed = "failed"


class InferenceRun(Base):
    __tablename__ = "inference_runs"

    id: Mapped[str] = mapped_column(String, primary_key=True, default=_uuid)
    deployment_id: Mapped[Optional[str]] = mapped_column(ForeignKey("deployments.id"), nullable=True)
    connector_id: Mapped[str] = mapped_column(ForeignKey("connectors.id"))
    question: Mapped[str] = mapped_column(Text)
    status: Mapped[str] = mapped_column(String, default=RunStatus.planning.value)
    plan: Mapped[dict[str, object] | None] = mapped_column(JSON, nullable=True)
    approved_sql: Mapped[Optional[str]] = mapped_column(Text, nullable=True)
    result_path: Mapped[Optional[str]] = mapped_column(String, nullable=True)
    created_at: Mapped[datetime] = mapped_column(default=utcnow)

    deployment: Mapped[Deployment | None] = relationship("Deployment")
    connector: Mapped[Connector] = relationship("Connector")
