from __future__ import annotations

import json
from pathlib import Path
from typing import Any

from sqlalchemy import desc, select
from sqlalchemy.ext.asyncio import AsyncSession

from openai import AsyncOpenAI, OpenAIError

from sqlglot import ParseError, parse_one
from sqlfluff.core import Linter

from nl2sql.config import get_settings
from nl2sql.models import (
    Connector,
    Deployment,
    InferenceRun,
    RunStatus,
    SchemaSnapshot,
    TrainingRun,
)
from nl2sql.observability import log_manager
from nl2sql.service.connectors import execute_sql

_linter = Linter(dialect="ansi")


def _format_sql(sql: str) -> str:
    try:
        fixed = _linter.fix_string(sql)
        return fixed.strip()
    except Exception:
        return sql
    

def parse_json(message: str) -> tuple[str, str]:
    try:
        data = json.loads(message)
        sql_code = data.get("sql_code")
        explain = data.get("explain", "")
        if not isinstance(sql_code, str) or not sql_code.strip():
            raise ValueError("Invalid or missing 'sql_code' in JSON response.")
        if not isinstance(explain, str):
            explain = ""
        return sql_code.strip(), explain.strip()
    except json.JSONDecodeError as exc:
        if "```" in message:
            message = message.replace("```json", "").replace("```", "")
            return parse_json(message)
        raise ValueError(f"Failed to parse JSON response: {exc}") from exc


async def _explain_summary(connector: Connector, sql: str) -> tuple[str, float]:
    try:
        rows = await execute_sql(connector.dsn, f"EXPLAIN QUERY PLAN {sql}", limit=5)
        summary = "; ".join(str(row.get("detail", "")) for row in rows)
        return summary or "explain unavailable", float(len(rows))
    except Exception:
        return "explain unavailable", 0.0


def _format_schema_summary(artifact: dict[str, Any]) -> str | None:
    rows = artifact.get("tables")
    if not isinstance(rows, list):
        return None

    detailed_rows = [row for row in rows if isinstance(row, dict) and row.get("columns")]

    if detailed_rows:
        table_lines: list[str] = []
        for row in detailed_rows:
            name = row.get("name") or row.get("table_name")
            if not isinstance(name, str) or not name:
                continue
            table_type = row.get("type") if isinstance(row.get("type"), str) else "table"

            columns_raw = row.get("columns")
            column_descs: list[str] = []
            if isinstance(columns_raw, list):
                for column in columns_raw:
                    if not isinstance(column, dict):
                        continue
                    col_name = column.get("name")
                    if not isinstance(col_name, str) or not col_name:
                        continue
                    parts = [col_name]
                    data_type = column.get("data_type")
                    if isinstance(data_type, str) and data_type:
                        parts.append(data_type)
                    if column.get("primary_key"):
                        parts.append("PK")
                    if not column.get("nullable", True):
                        parts.append("NOT NULL")
                    default_value = column.get("default_value")
                    if default_value is not None:
                        parts.append(f"default={default_value}")
                    column_descs.append(" ".join(parts))

            fk_entries = row.get("foreign_keys")
            fk_descs: list[str] = []
            if isinstance(fk_entries, list):
                for fk in fk_entries:
                    if not isinstance(fk, dict):
                        continue
                    from_columns = [
                        col
                        for col in fk.get("columns", [])
                        if isinstance(col, str) and col
                    ]
                    references = fk.get("references")
                    if not isinstance(references, dict):
                        continue
                    to_table = references.get("table")
                    to_columns = [
                        col
                        for col in references.get("columns", [])
                        if isinstance(col, str) and col
                    ]
                    if not isinstance(to_table, str):
                        continue

                    from_repr = ", ".join(from_columns) if from_columns else "?"
                    to_repr = ", ".join(to_columns) if to_columns else "?"
                    clause = f"{from_repr} -> {to_table}({to_repr})"

                    actions: list[str] = []
                    for key in ("on_update", "on_delete", "match"):
                        value = fk.get(key)
                        if isinstance(value, str) and value.upper() != "NONE":
                            actions.append(f"{key}={value}")
                    if actions:
                        clause += f" [{' '.join(actions)}]"
                    fk_descs.append(clause)

            table_lines.append(f"- {name} ({table_type})")
            if column_descs:
                table_lines.append(f"  columns: {', '.join(column_descs)}")
            if fk_descs:
                table_lines.append(f"  foreign_keys: {'; '.join(fk_descs)}")

        relationship_lines: list[str] = []
        relationships_raw = artifact.get("relationships")
        if isinstance(relationships_raw, list):
            for relationship in relationships_raw:
                if not isinstance(relationship, dict):
                    continue
                from_info = relationship.get("from")
                to_info = relationship.get("to")
                if not isinstance(from_info, dict) or not isinstance(to_info, dict):
                    continue
                from_table = from_info.get("table")
                to_table = to_info.get("table")
                if not isinstance(from_table, str) or not isinstance(to_table, str):
                    continue

                from_columns = [
                    col
                    for col in from_info.get("columns", [])
                    if isinstance(col, str) and col
                ]
                to_columns = [
                    col
                    for col in to_info.get("columns", [])
                    if isinstance(col, str) and col
                ]

                from_repr = f"{from_table}({', '.join(from_columns)})" if from_columns else from_table
                to_repr = f"{to_table}({', '.join(to_columns)})" if to_columns else to_table

                modifiers: list[str] = []
                for key in ("on_update", "on_delete", "match"):
                    value = relationship.get(key)
                    if isinstance(value, str) and value.upper() != "NONE":
                        modifiers.append(f"{key}={value}")

                suffix = f" [{' '.join(modifiers)}]" if modifiers else ""
                relationship_lines.append(f"- {from_repr} -> {to_repr}{suffix}")

        sections: list[str] = []
        if table_lines:
            sections.append("Tables:\n" + "\n".join(table_lines))
        if relationship_lines:
            sections.append("Relationships:\n" + "\n".join(relationship_lines))
        if sections:
            return "\n\n".join(sections)

    table_names: list[str] = []
    for row in rows:
        if not isinstance(row, dict):
            continue
        name = row.get("name") or row.get("table_name")
        if isinstance(name, str) and name:
            table_names.append(name)

    if not table_names:
        return None

    return "\n".join(f"- {name}" for name in table_names)


async def _load_schema_overview(
    session: AsyncSession, deployment: Deployment | None
) -> str | None:
    if deployment is None or not isinstance(deployment, Deployment):
        return None

    training = await session.get(TrainingRun, deployment.run_id)
    if training is None or training.schema_snapshot_id is None:
        return None

    snapshot = await session.get(SchemaSnapshot, training.schema_snapshot_id)
    if snapshot is None or snapshot.artifact_path is None:
        return None

    artifact_path = Path(snapshot.artifact_path)
    if not artifact_path.exists():
        return None

    try:
        artifact = json.loads(artifact_path.read_text())
    except json.JSONDecodeError:
        return None

    return _format_schema_summary(artifact)


async def _generate_stub_plan(_question: str) -> tuple[list[tuple[str, str]], list[str]]:
    raw_candidates = [
        (
            "SELECT 1 AS answer",
            "Baseline safe query to validate pipeline",
        ),
        (
            "SELECT COUNT(*) AS row_count FROM sqlite_master",
            "Estimate table count using SQLite metadata",
        ),
    ]
    clarifications = [
        "Confirm table and column names referenced in the plan before approval.",
        "Provide table schemas so the system can tailor SQL more accurately.",
    ]
    return raw_candidates, clarifications


async def _generate_openai_plan(
    session: AsyncSession,
    *,
    deployment: Deployment | None,
    connector: Connector,
    question: str,
) -> tuple[list[tuple[str, str]], list[str]]:
    settings = get_settings()
    if not settings.openai_api_key:
        raise ValueError("OpenAI API key is not configured.")

    # schema_overview = await _load_schema_overview(session, deployment)  # TODO: enable logic here, currently hardcoded
    schema_overview: str = open("/Users/yamazakij/Repos/nl2sql/data/artifacts/schemas/13a88f5d-df8c-4886-ab0b-5474378c981a.dbml", "r").read()
    schema_section = (
        f"Schema overview for connector:\n{schema_overview}"
    )

    system_prompt = (
        "You are an expert data analyst that writes safe SQLite SQL statements. "
        "Respond as a plain JSON with two keys: 'sql_code' and 'explain'."
        "The 'sql_code' value is a string containing the SQL statement you propose. "
        "The 'explain' value is a string containing any explanation or rationale for the SQL code."
    )

    user_prompt = (
        f"User question:\n{question}\n\n"
        f"Database context:\n{schema_section}\n\n"
        "Ensure generated SQL references existing tables when possible and include LIMIT clauses "
        "only when they improve safety."
    )

    client = AsyncOpenAI(api_key=settings.openai_api_key, base_url=settings.openai_api_base)
    try:
        response = await client.chat.completions.create(
            model=settings.openai_model,
            temperature=settings.openai_temperature,
            messages=[
                {"role": "system", "content": system_prompt},
                {"role": "user", "content": user_prompt},
            ],
        )
    except OpenAIError as exc:  # pragma: no cover - network error path
        raise ValueError(f"OpenAI request failed: {exc}") from exc
    finally:
        await client.close()

    message = response.choices[0].message.content if response.choices else None
    if not message:
        raise ValueError("OpenAI response did not contain any content.")
    
    code, rationale = parse_json(message)
    return code, rationale


async def plan_inference(
    session: AsyncSession,
    *,
    deployment: Deployment,
    connector: Connector,
    question: str,
    provider: str | None = None,
) -> tuple[InferenceRun, list[dict[str, Any]], list[str]]:
    run = InferenceRun(
        deployment_id=deployment.id,
        connector_id=connector.id,
        question=question,
    )
    session.add(run)
    await session.flush()

    settings = get_settings()
    selected_provider = provider or settings.inference_provider
    if selected_provider == "openai":
        code, rationale = await _generate_openai_plan(
            session,
            deployment=deployment,
            connector=connector,
            question=question,
        )
    else:
        raw_candidates, clarifications = await _generate_stub_plan(question)

    run.status = RunStatus.awaiting_approval.value
    run.plan = {"candidates": code, "clarifications": rationale}
    await session.flush()

    await log_manager.emit(run.id, "plan generated")

    return run, code, rationale


async def execute_inference(
    session: AsyncSession,
    *,
    run: InferenceRun,
    connector: Connector,
    approved_sql: str,
    limit: int,
) -> tuple[InferenceRun, list[dict[str, Any]]]:
    try:
        expression = parse_one(approved_sql)
    except ParseError as exc:
        raise ValueError(f"SQL validation failed: {exc}")

    formatted_sql = _format_sql(approved_sql)

    run.status = RunStatus.executing.value
    await session.flush()

    rows = await execute_sql(connector.dsn, formatted_sql, limit)

    settings = get_settings()
    result_dir = settings.object_store_path / "runs"
    result_dir.mkdir(parents=True, exist_ok=True)
    result_path = result_dir / f"{run.id}.json"
    result_path.write_text(json.dumps({"rows": rows}))

    run.status = RunStatus.completed.value
    run.approved_sql = formatted_sql
    run.result_path = str(result_path)
    await session.flush()

    await log_manager.emit(run.id, f"executed approved SQL; rows={len(rows)}")

    return run, rows


async def get_inference_run(session: AsyncSession, run_id: str) -> InferenceRun:
    run = await session.get(InferenceRun, run_id)
    if run is None:
        raise ValueError("Inference run not found")
    return run


async def list_inference_runs(session: AsyncSession, limit: int) -> list[InferenceRun]:
    result = await session.execute(
        select(InferenceRun).order_by(desc(InferenceRun.created_at)).limit(limit)
    )
    return list(result.scalars())
