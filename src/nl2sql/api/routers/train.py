from __future__ import annotations

from fastapi import APIRouter, Depends, HTTPException, status
from sqlalchemy.ext.asyncio import AsyncSession

from nl2sql.api.dependencies.database import get_session
from nl2sql.api.schemas import TrainingCreate, TrainingResponse
from nl2sql.jobs.queue import enqueue
from nl2sql.observability import log_manager
from nl2sql.service.projects import get_project
from nl2sql.service.snapshots import get_snapshot
from nl2sql.service.training import create_training_run, get_training_run

router = APIRouter()


@router.post("/train", response_model=TrainingResponse, status_code=status.HTTP_202_ACCEPTED)
async def create_training(
    payload: TrainingCreate, session: AsyncSession = Depends(get_session)
) -> TrainingResponse:
    await get_project(session, payload.project)
    await get_snapshot(session, payload.schema_snapshot)

    training = await create_training_run(
        session,
        project_id=payload.project,
        schema_snapshot_id=payload.schema_snapshot,
        config_path=payload.config_ref,
        job_id=None,
    )
    await session.flush()

    job_id = await enqueue("training_run_job", run_id=training.id)
    training.job_id = job_id
    await session.commit()

    await log_manager.emit(training.id, "training run enqueued")
    return TrainingResponse.model_validate(training)


@router.get("/train/{run_id}", response_model=TrainingResponse)
async def get_training(run_id: str, session: AsyncSession = Depends(get_session)) -> TrainingResponse:
    training = await get_training_run(session, run_id)
    return TrainingResponse.model_validate(training)
