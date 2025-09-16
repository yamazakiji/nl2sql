from __future__ import annotations

from fastapi import APIRouter, Depends, HTTPException, status
from sqlalchemy.exc import IntegrityError
from sqlalchemy.ext.asyncio import AsyncSession

from nl2sql.api.schemas import ProjectCreate, ProjectResponse
from nl2sql.api.dependencies.database import get_session
from nl2sql.service.projects import create_project

router = APIRouter()


@router.post("/projects", response_model=ProjectResponse, status_code=status.HTTP_201_CREATED)
async def create_project_endpoint(
    payload: ProjectCreate, session: AsyncSession = Depends(get_session)
) -> ProjectResponse:
    try:
        project = await create_project(session, payload.name)
    except IntegrityError as exc:
        raise HTTPException(status_code=400, detail="Project name already exists") from exc
    await session.commit()
    return ProjectResponse.model_validate(project)
