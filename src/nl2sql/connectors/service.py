from __future__ import annotations

from typing import Any

from sqlalchemy import text
from sqlalchemy.engine import make_url
from sqlalchemy.exc import SQLAlchemyError
from sqlalchemy.ext.asyncio import create_async_engine

from nl2sql.observability import logger

from .utils import ensure_sqlite_read_only


class ConnectorError(Exception):
    pass


async def test_connector(dsn: str) -> None:
    url = make_url(dsn)
    if not url.drivername.startswith("sqlite"):
        raise ConnectorError("Connector testing is only implemented for SQLite in this starter.")

    read_only_dsn = ensure_sqlite_read_only(dsn)
    engine = create_async_engine(read_only_dsn, future=True)
    try:
        async with engine.connect() as conn:
            await conn.execute(text("SELECT 1"))
    except SQLAlchemyError as exc:
        raise ConnectorError(str(exc)) from exc
    finally:
        await engine.dispose()


async def execute_sql(dsn: str, sql: str, limit: int) -> list[dict[str, Any]]:
    url = make_url(dsn)
    if not url.drivername.startswith("sqlite"):
        raise ConnectorError("SQL execution is only implemented for SQLite in this starter.")

    read_only_dsn = ensure_sqlite_read_only(dsn)
    engine = create_async_engine(read_only_dsn, future=True)
    try:
        async with engine.connect() as conn:
            result = await conn.execute(text(sql))
            rows = result.mappings().all()
    except SQLAlchemyError as exc:
        raise ConnectorError(str(exc)) from exc
    finally:
        await engine.dispose()

    limited = rows[:limit]
    payload = [dict(row) for row in limited]
    logger.info("connector.execute", row_count=len(payload))
    return payload
