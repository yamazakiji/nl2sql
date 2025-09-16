from __future__ import annotations

from sqlalchemy.ext.asyncio import AsyncSession

from nl2sql.models import Project


async def create_project(session: AsyncSession, name: str) -> Project:
    project = Project(name=name)
    session.add(project)
    await session.flush()
    return project


async def get_project(session: AsyncSession, project_id: str) -> Project:
    project = await session.get(Project, project_id)
    if project is None:
        raise ValueError("Project not found")
    return project
