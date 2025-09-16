from __future__ import annotations

from sqlalchemy.ext.asyncio import AsyncSession

from nl2sql.models import TrainingRun, TrainingStatus


async def create_training_run(
    session: AsyncSession,
    *,
    project_id: str,
    schema_snapshot_id: str,
    config_path: str,
    job_id: str | None,
) -> TrainingRun:
    training = TrainingRun(
        project_id=project_id,
        schema_snapshot_id=schema_snapshot_id,
        config_path=config_path,
        job_id=job_id,
    )
    session.add(training)
    await session.flush()
    return training


async def get_training_run(session: AsyncSession, run_id: str) -> TrainingRun:
    run = await session.get(TrainingRun, run_id)
    if run is None:
        raise ValueError("Training run not found")
    return run


async def update_training_status(
    session: AsyncSession, run_id: str, status: TrainingStatus, metrics: dict[str, object] | None = None
) -> TrainingRun:
    run = await get_training_run(session, run_id)
    run.status = status.value
    if metrics is not None:
        run.metrics = metrics
    await session.flush()
    return run
