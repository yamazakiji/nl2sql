from __future__ import annotations

from sqlalchemy import MetaData
from sqlalchemy.engine import make_url
from sqlalchemy.sql.sqltypes import NullType
from sqlalchemy.ext.asyncio import AsyncSession, create_async_engine

from nl2sql.config import get_settings
from nl2sql.connectors.utils import ensure_sqlite_read_only
from nl2sql.models import Connector, SchemaSnapshot, SchemaSnapshotStatus


async def create_snapshot(session: AsyncSession, connector_id: str, job_id: str | None) -> SchemaSnapshot:
    connector = await session.get(Connector, connector_id)
    if connector is None:
        raise ValueError("Connector not found for snapshot creation")

    url = make_url(connector.dsn)
    if not url.drivername.startswith("sqlite"):
        raise ValueError("DBML snapshots currently support only SQLite connectors")

    snapshot = SchemaSnapshot(connector_id=connector_id, job_id=job_id)
    session.add(snapshot)
    await session.flush()

    engine = create_async_engine(ensure_sqlite_read_only(connector.dsn), future=True)
    metadata = MetaData()

    try:
        async with engine.begin() as conn:
            await conn.run_sync(lambda sync_conn: metadata.reflect(bind=sync_conn, views=True))

        dialect = engine.sync_engine.dialect
        tables = [table for table in metadata.sorted_tables if not table.name.startswith("sqlite_")]

        def _fmt_identifier(name: str) -> str:
            escaped = name.replace("\"", "\\\"")
            return f'"{escaped}"'

        def _format_columns(columns: list) -> str:
            if not columns:
                return ""
            if len(columns) == 1:
                return f".{_fmt_identifier(columns[0])}"
            joined = ", ".join(_fmt_identifier(col) for col in columns)
            return f".({joined})"

        lines: list[str] = []
        for table in tables:
            table_name = _fmt_identifier(table.name)
            lines.append(f"Table {table_name} {{")
            for column in table.columns:
                if isinstance(column.type, NullType):
                    column_type = "NULL"
                else:
                    column_type = column.type.compile(dialect=dialect)
                attributes: list[str] = []
                if column.primary_key:
                    attributes.append("pk")
                if not column.nullable:
                    attributes.append("not null")
                if column.unique:
                    attributes.append("unique")

                default_clause = None
                server_default = column.server_default
                if server_default is not None:
                    default_value = getattr(server_default.arg, "text", None)
                    if default_value is None and server_default.arg is not None:
                        default_value = str(server_default.arg)
                    if default_value:
                        default_clause = f"default: {default_value.strip()}"

                properties = [prop for prop in [default_clause, *attributes] if prop]
                suffix = f" [{', '.join(properties)}]" if properties else ""
                column_name = _fmt_identifier(column.name)
                lines.append(f"  {column_name} {column_type}{suffix}")
            lines.append("}")
            lines.append("")

        references: list[str] = []
        seen_refs: set[str] = set()
        for table in tables:
            table_identifier = _fmt_identifier(table.name)
            for constraint in table.foreign_key_constraints:
                referred_table = constraint.referred_table
                if referred_table is None:
                    continue
                if referred_table.name.startswith("sqlite_"):
                    continue
                local_columns = [element.parent.name for element in constraint.elements]
                remote_columns = [
                    element.column.name
                    for element in constraint.elements
                    if element.column is not None
                ]
                if not local_columns or not remote_columns:
                    continue
                left = f"{table_identifier}{_format_columns(local_columns)}"
                right = f"{_fmt_identifier(referred_table.name)}{_format_columns(remote_columns)}"
                ref = f"Ref: {left} > {right}"
                if ref in seen_refs:
                    continue
                seen_refs.add(ref)
                references.append(ref)

        if references:
            lines.extend(references)

        dbml_text = "\n".join(line.rstrip() for line in lines).rstrip()
        if dbml_text:
            dbml_text += "\n"

        settings = get_settings()
        artifact_dir = settings.object_store_path / "schemas"
        artifact_dir.mkdir(parents=True, exist_ok=True)
        artifact_path = artifact_dir / f"{snapshot.id}.dbml"
        artifact_path.write_text(dbml_text or "")
        snapshot.artifact_path = str(artifact_path)
    finally:
        await engine.dispose()

    return snapshot


async def get_snapshot(session: AsyncSession, snapshot_id: str) -> SchemaSnapshot:
    snapshot = await session.get(SchemaSnapshot, snapshot_id)
    if snapshot is None:
        raise ValueError("Schema snapshot not found")
    return snapshot


async def update_snapshot_status(
    session: AsyncSession, snapshot_id: str, status: SchemaSnapshotStatus, artifact_path: str | None = None
) -> SchemaSnapshot:
    snapshot = await get_snapshot(session, snapshot_id)
    snapshot.status = status.value
    if artifact_path is not None:
        snapshot.artifact_path = artifact_path
    await session.flush()
    return snapshot
