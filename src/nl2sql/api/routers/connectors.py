from __future__ import annotations

from fastapi import APIRouter, Depends, HTTPException, status
from sqlalchemy.ext.asyncio import AsyncSession

from nl2sql.api.dependencies.database import get_session
from nl2sql.api.schemas import (
    ConnectorCreate,
    ConnectorResponse,
    ConnectorTestResponse,
    SchemaSnapshotResponse,
)
from nl2sql.jobs.queue import enqueue
from nl2sql.models import SchemaSnapshotStatus
from nl2sql.observability import log_manager
from nl2sql.service.connectors import (
    ConnectorError,
    create_connector,
    get_connector,
    mask_connector,
    test_connector,
)
from nl2sql.service.snapshots import create_snapshot, get_snapshot

router = APIRouter()


@router.post("/connectors", response_model=ConnectorResponse, status_code=status.HTTP_201_CREATED)
async def create_connector_endpoint(
    payload: ConnectorCreate,
    session: AsyncSession = Depends(get_session),
) -> ConnectorResponse:
    connector = await create_connector(
        session,
        type_=payload.type,
        name=payload.name,
        dsn=payload.dsn,
    )
    await session.commit()
    shaped = mask_connector(connector)
    await log_manager.emit(connector.id, "connector created")
    return ConnectorResponse(**shaped)


@router.post("/connectors/{connector_id}/test", response_model=ConnectorTestResponse)
async def test_connector_endpoint(
    connector_id: str,
    session: AsyncSession = Depends(get_session),
) -> ConnectorTestResponse:
    connector = await get_connector(session, connector_id)
    try:
        await test_connector(connector.dsn)
    except ConnectorError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc
    return ConnectorTestResponse(status="ok", details="Connection verified")


@router.post("/connectors/{connector_id}/rotate-credentials")
async def rotate_connector_credentials(connector_id: str) -> dict[str, str]:
    # TODO: impelement in future
    await log_manager.emit(connector_id, "credential rotation requested")
    return {"status": "noop", "details": "Credential rotation is not yet implemented."}


@router.post(
    "/connectors/{connector_id}/schema/snapshot",
    response_model=SchemaSnapshotResponse,
    status_code=status.HTTP_202_ACCEPTED,
)
async def snapshot_schema(
    connector_id: str,
    session: AsyncSession = Depends(get_session),
) -> SchemaSnapshotResponse:
    connector = await get_connector(session, connector_id)
    snapshot = await create_snapshot(session, connector.id, job_id=None)
    await session.flush()

    job_id = await enqueue("schema_snapshot_job", snapshot_id=snapshot.id, connector_id=connector.id)

    snapshot.job_id = job_id
    snapshot.status = SchemaSnapshotStatus.queued.value
    await session.commit()

    await log_manager.emit(snapshot.id, "schema snapshot enqueued")

    return SchemaSnapshotResponse(
        id=snapshot.id,
        connector_id=connector.id,
        status=snapshot.status,
        job_id=job_id,
        artifact_path=snapshot.artifact_path,
        created_at=snapshot.created_at,
    )


@router.get("/schema-snapshots/{snapshot_id}", response_model=SchemaSnapshotResponse)
async def get_schema_snapshot(snapshot_id: str, session: AsyncSession = Depends(get_session)) -> SchemaSnapshotResponse:
    snapshot = await get_snapshot(session, snapshot_id)
    return SchemaSnapshotResponse.model_validate(snapshot)
