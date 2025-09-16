from __future__ import annotations

from fastapi import APIRouter, Depends, status
from sqlalchemy.ext.asyncio import AsyncSession

from nl2sql.api.dependencies.database import get_session
from nl2sql.api.schemas import DeploymentCreate, DeploymentResponse
from nl2sql.observability import log_manager
from nl2sql.service.deployments import create_deployment

router = APIRouter()


@router.post("/deployments", response_model=DeploymentResponse, status_code=status.HTTP_201_CREATED)
async def create_deployment_endpoint(
    payload: DeploymentCreate, session: AsyncSession = Depends(get_session)
) -> DeploymentResponse:
    deployment = await create_deployment(session, payload.run, payload.label)
    await session.commit()
    await log_manager.emit(deployment.id, "deployment activated")
    return DeploymentResponse.model_validate(deployment)
