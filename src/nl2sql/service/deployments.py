from __future__ import annotations

from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from nl2sql.models import Deployment, DeploymentStatus, TrainingRun


async def create_deployment(session: AsyncSession, run_id: str, label: str) -> Deployment:
    training = await session.get(TrainingRun, run_id)
    if training is None:
        # openai deployment
        deployment = Deployment(project_id="OPENAI", run_id=run_id, label=label)
        session.add(deployment)
        await session.flush()
        return deployment

    # Deactivate any existing deployments with this label
    existing = await session.execute(select(Deployment).where(Deployment.label == label))
    for deployment in existing.scalars():
        deployment.status = DeploymentStatus.inactive.value

    deployment = Deployment(project_id=training.project_id, run_id=run_id, label=label)
    session.add(deployment)
    await session.flush()
    return deployment


async def get_deployment(session: AsyncSession, identifier: str) -> Deployment:
    deployment = await session.get(Deployment, identifier)
    if deployment:
        return deployment

    result = await session.execute(select(Deployment).where(Deployment.label == identifier))
    deployment = result.scalars().first()
    if deployment is None:
        raise ValueError("Deployment not found")
    return deployment
