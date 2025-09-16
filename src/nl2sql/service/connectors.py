from __future__ import annotations

from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from nl2sql.connectors.service import ConnectorError, execute_sql, test_connector
from nl2sql.connectors.utils import ensure_sqlite_read_only, mask_dsn
from nl2sql.models import Connector


async def create_connector(session: AsyncSession, *, type_: str, name: str, dsn: str) -> Connector:
    sanitized_dsn = ensure_sqlite_read_only(dsn) if type_ == "sqlite" else dsn
    connector = Connector(type=type_, name=name, dsn=sanitized_dsn)
    session.add(connector)
    await session.flush()
    return connector


async def get_connector(session: AsyncSession, connector_id: str) -> Connector:
    connector = await session.get(Connector, connector_id)
    if connector is None:
        raise ValueError("Connector not found")
    return connector


async def list_connectors(session: AsyncSession) -> list[Connector]:
    result = await session.execute(select(Connector))
    return list(result.scalars())


def mask_connector(connector: Connector) -> dict[str, object]:
    return {
        "id": connector.id,
        "type": connector.type,
        "name": connector.name,
        "created_at": connector.created_at,
        "dsn_masked": mask_dsn(connector.dsn),
    }


__all__ = [
    "create_connector",
    "get_connector",
    "list_connectors",
    "mask_connector",
    "ConnectorError",
    "execute_sql",
    "test_connector",
]
