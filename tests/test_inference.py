from __future__ import annotations

import os
import sqlite3
from pathlib import Path

import pytest
from httpx import AsyncClient


def setup_connector_db(base_dir: Path) -> Path:
    db_path = base_dir / "source.db"
    with sqlite3.connect(db_path) as conn:
        conn.execute("CREATE TABLE IF NOT EXISTS users (id INTEGER PRIMARY KEY, name TEXT)")
        conn.executemany("INSERT INTO users(name) VALUES (?)", [("alice",), ("bob",)])
        conn.commit()
    return db_path


async def test_plan_and_execute_flow(client: AsyncClient) -> None:
    tmp_dir = Path(os.environ["NL2SQL_TEST_TMP"])
    source_db = setup_connector_db(tmp_dir)

    project_resp = await client.post("/projects", json={"name": "demo"})
    assert project_resp.status_code == 201
    project_id = project_resp.json()["id"]

    connector_resp = await client.post(
        "/connectors",
        json={"type": "sqlite", "name": "demo", "dsn": f"sqlite+aiosqlite:///{source_db}"},
    )
    assert connector_resp.status_code == 201
    connector_id = connector_resp.json()["id"]

    snapshot_resp = await client.post(f"/connectors/{connector_id}/schema/snapshot")
    assert snapshot_resp.status_code == 202
    snapshot_id = snapshot_resp.json()["id"]

    train_resp = await client.post(
        "/train",
        json={"project": project_id, "schema_snapshot": snapshot_id, "config_ref": "train.yaml"},
    )
    assert train_resp.status_code == 202
    training_id = train_resp.json()["id"]

    deployment_resp = await client.post(
        "/deployments", json={"run": training_id, "label": "dev"}
    )
    assert deployment_resp.status_code == 201

    plan_resp = await client.post(
        "/inference/plan",
        json={"question": "count users", "deployment": "dev", "connector": connector_id},
    )
    assert plan_resp.status_code == 200
    plan_payload = plan_resp.json()
    assert plan_payload["candidates"]

    candidate_sql = plan_payload["candidates"][0]["sql"]

    execute_resp = await client.post(
        "/inference/execute",
        json={
            "run_id": plan_payload["run_id"],
            "connector": connector_id,
            "approved_sql": candidate_sql,
            "limit": 5,
        },
    )
    assert execute_resp.status_code == 200
    execute_payload = execute_resp.json()
    assert "rows" in execute_payload
    assert execute_payload["row_count"] >= 0


async def test_plan_with_openai_provider(client: AsyncClient, monkeypatch: pytest.MonkeyPatch) -> None:
    tmp_dir = Path(os.environ["NL2SQL_TEST_TMP"])
    source_db = setup_connector_db(tmp_dir)

    connector_resp = await client.post(
        "/connectors",
        json={"type": "sqlite", "name": "demo", "dsn": f"sqlite+aiosqlite:///{source_db}"},
    )
    assert connector_resp.status_code == 201
    connector_id = connector_resp.json()["id"]

    async def fake_generate(
        session, *, deployment, connector, question
    ) -> tuple[list[tuple[str, str]], list[str]]:
        assert question == "show users"
        return [
            ("SELECT id, name FROM users ORDER BY id", "LLM-proposed query"),
        ], ["Verify column coverage with stakeholders."]

    monkeypatch.setattr("nl2sql.service.inference._generate_openai_plan", fake_generate)

    plan_resp = await client.post(
        "/inference/plan",
        json={"question": "show users", "deployment": "openai", "connector": connector_id},
    )
    assert plan_resp.status_code == 200
    payload = plan_resp.json()
    assert payload["candidates"][0]["sql"] == "SELECT id, name FROM users ORDER BY id"
    assert payload["candidates"][0]["rationale"] == "LLM-proposed query"
    assert payload["clarifications"] == ["Verify column coverage with stakeholders."]

    execute_resp = await client.post(
        "/inference/execute",
        json={
            "run_id": payload["run_id"],
            "connector": connector_id,
            "approved_sql": payload["candidates"][0]["sql"],
            "limit": 5,
        },
    )
    assert execute_resp.status_code == 200
