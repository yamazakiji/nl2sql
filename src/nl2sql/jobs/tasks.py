from __future__ import annotations

import json
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

from sqlalchemy import text
from sqlalchemy.engine import make_url
from sqlalchemy.ext.asyncio import create_async_engine

from nl2sql.config import get_settings
from nl2sql.connectors.utils import ensure_sqlite_read_only
from nl2sql.db import session_scope
from nl2sql.models import Connector, SchemaSnapshotStatus, TrainingStatus
from nl2sql.observability import logger
from nl2sql.service.snapshots import get_snapshot, update_snapshot_status
from nl2sql.service.training import get_training_run, update_training_status


def _escape_identifier(name: str) -> str:
    return name.replace("\"", "\"\"")


async def _generate_sqlite_schema(dsn: str) -> dict[str, Any]:
    url = make_url(dsn)
    if not url.drivername.startswith("sqlite"):
        raise ValueError("Schema snapshots currently support only SQLite connectors.")

    read_only_dsn = ensure_sqlite_read_only(dsn)
    engine = create_async_engine(read_only_dsn, future=True)

    try:
        async with engine.connect() as conn:
            tables_result = await conn.execute(
                text(
                    "SELECT name, type FROM sqlite_master "
                    "WHERE type IN ('table', 'view') AND name NOT LIKE 'sqlite_%' "
                    "ORDER BY name"
                )
            )
            tables: list[dict[str, Any]] = []

            for table_row in tables_result.mappings():
                table_name = table_row.get("name")
                table_type = table_row.get("type")
                if not isinstance(table_name, str) or not table_name:
                    continue

                escaped_table = _escape_identifier(table_name)

                columns_result = await conn.execute(text(f'PRAGMA table_info("{escaped_table}")'))
                column_entries = columns_result.mappings().all()

                columns: list[dict[str, Any]] = []
                primary_key_parts: list[tuple[int, str]] = []
                for col in column_entries:
                    column_name = col.get("name")
                    if not isinstance(column_name, str):
                        continue

                    pk_position = int(col.get("pk") or 0)
                    if pk_position > 0:
                        primary_key_parts.append((pk_position, column_name))

                    columns.append(
                        {
                            "name": column_name,
                            "data_type": col.get("type"),
                            "nullable": not bool(col.get("notnull")),
                            "default_value": col.get("dflt_value"),
                            "primary_key": pk_position > 0,
                            "primary_key_position": pk_position if pk_position > 0 else None,
                        }
                    )

                foreign_keys_result = await conn.execute(
                    text(f'PRAGMA foreign_key_list("{escaped_table}")')
                )
                fk_rows = foreign_keys_result.mappings().all()

                fk_groups: dict[int, dict[str, Any]] = {}
                for fk in sorted(
                    fk_rows,
                    key=lambda row: (int(row.get("id") or 0), int(row.get("seq") or 0)),
                ):
                    fk_id_raw = fk.get("id")
                    if fk_id_raw is None:
                        continue
                    fk_id = int(fk_id_raw)
                    group = fk_groups.setdefault(
                        fk_id,
                        {
                            "columns": [],
                            "references": {
                                "table": fk.get("table"),
                                "columns": [],
                            },
                            "on_update": fk.get("on_update"),
                            "on_delete": fk.get("on_delete"),
                            "match": fk.get("match"),
                        },
                    )

                    from_column = fk.get("from")
                    to_column = fk.get("to")
                    if isinstance(from_column, str):
                        group["columns"].append(from_column)
                    if isinstance(to_column, str):
                        group["references"]["columns"].append(to_column)

                    referenced_table = fk.get("table")
                    if isinstance(referenced_table, str):
                        group["references"]["table"] = referenced_table

                foreign_keys = list(fk_groups.values())

                index_result = await conn.execute(text(f'PRAGMA index_list("{escaped_table}")'))
                index_entries = index_result.mappings().all()
                indexes: list[dict[str, Any]] = []

                for index in index_entries:
                    index_name = index.get("name")
                    if not isinstance(index_name, str) or not index_name:
                        continue

                    escaped_index = _escape_identifier(index_name)
                    index_info_result = await conn.execute(
                        text(f'PRAGMA index_info("{escaped_index}")')
                    )
                    index_info_rows = index_info_result.mappings().all()
                    index_columns = [
                        info.get("name")
                        for info in sorted(index_info_rows, key=lambda row: row.get("seqno") or 0)
                        if isinstance(info.get("name"), str)
                    ]

                    indexes.append(
                        {
                            "name": index_name,
                            "unique": bool(index.get("unique")),
                            "origin": index.get("origin"),
                            "partial": bool(index.get("partial")),
                            "columns": index_columns,
                        }
                    )

                table_payload = {
                    "name": table_name,
                    "type": table_type,
                    "columns": columns,
                    "primary_key": [name for _, name in sorted(primary_key_parts)],
                    "foreign_keys": foreign_keys,
                    "indexes": indexes,
                }
                tables.append(table_payload)

            relationships: list[dict[str, Any]] = []
            for table in tables:
                table_name = table.get("name")
                for fk in table.get("foreign_keys", []):
                    columns = fk.get("columns") if isinstance(fk, dict) else None
                    references = fk.get("references") if isinstance(fk, dict) else None
                    if not isinstance(columns, list) or not isinstance(references, dict):
                        continue
                    ref_table = references.get("table")
                    ref_columns = references.get("columns")
                    if not isinstance(table_name, str) or not isinstance(ref_table, str):
                        continue
                    if not isinstance(ref_columns, list):
                        continue
                    relationships.append(
                        {
                            "from": {"table": table_name, "columns": [c for c in columns if isinstance(c, str)]},
                            "to": {
                                "table": ref_table,
                                "columns": [c for c in ref_columns if isinstance(c, str)],
                            },
                            "on_update": fk.get("on_update"),
                            "on_delete": fk.get("on_delete"),
                            "match": fk.get("match"),
                        }
                    )

            payload: dict[str, Any] = {
                "generated_at": datetime.now(timezone.utc).isoformat(),
                "database": {
                    "driver": url.drivername,
                    "dialect": url.get_backend_name(),
                },
                "tables": tables,
                "relationships": relationships,
            }
            return payload
    finally:
        await engine.dispose()


async def schema_snapshot_job(ctx: dict[str, object], snapshot_id: str, connector_id: str) -> str:
    settings = get_settings()
    async with session_scope() as session:
        snapshot = await get_snapshot(session, snapshot_id)
        snapshot.status = SchemaSnapshotStatus.running.value
        await session.flush()

        connector = await session.get(Connector, connector_id)
        if connector is None:
            raise ValueError("Connector not found for snapshot job")
        dsn = connector.dsn

    try:
        schema_payload = await _generate_sqlite_schema(dsn)
    except Exception as exc:  # pragma: no cover - runtime error path
        async with session_scope() as session:
            await update_snapshot_status(session, snapshot_id, SchemaSnapshotStatus.failed)
        logger.error("jobs.schema_snapshot_failed", snapshot_id=snapshot_id, error=str(exc))
        raise

    artifact_dir = settings.object_store_path / "schemas"
    artifact_dir.mkdir(parents=True, exist_ok=True)
    artifact_path = artifact_dir / f"{snapshot_id}.json"
    artifact_path.write_text(json.dumps(schema_payload, indent=2))

    async with session_scope() as session:
        await update_snapshot_status(
            session,
            snapshot_id,
            SchemaSnapshotStatus.completed,
            artifact_path=str(artifact_path),
        )

    logger.info("jobs.schema_snapshot_completed", snapshot_id=snapshot_id)
    return str(artifact_path)


async def training_run_job(ctx: dict[str, object], run_id: str) -> str:
    async with session_scope() as session:
        training = await get_training_run(session, run_id)
        training.status = TrainingStatus.running.value
        await session.flush()

    metrics = {"placeholder_accuracy": 0.0}

    async with session_scope() as session:
        await update_training_status(session, run_id, TrainingStatus.completed, metrics=metrics)

    logger.info("jobs.training_run_completed", run_id=run_id)
    return "completed"
