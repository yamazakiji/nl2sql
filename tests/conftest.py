from __future__ import annotations

import sys
from collections.abc import AsyncIterator
from pathlib import Path

import pytest
from httpx import ASGITransport, AsyncClient


@pytest.fixture()
async def client(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> AsyncIterator[AsyncClient]:
    database_path = tmp_path / "metadata.db"
    artifacts_path = tmp_path / "artifacts"

    monkeypatch.setenv("NL2SQL_DATABASE_URL", f"sqlite+aiosqlite:///{database_path}")
    monkeypatch.setenv("NL2SQL_OBJECT_STORE_PATH", str(artifacts_path))
    monkeypatch.setenv("NL2SQL_REDIS_URL", "redis://localhost:6379/0")
    monkeypatch.setenv("NL2SQL_TEST_TMP", str(tmp_path))

    for module in list(sys.modules):
        if module.startswith("nl2sql"):
            del sys.modules[module]

    from nl2sql.config import get_settings

    get_settings.cache_clear()

    from nl2sql.api.app import create_app

    app = create_app()

    transport = ASGITransport(app=app)
    await app.router.startup()
    try:
        async with AsyncClient(transport=transport, base_url="http://testserver") as http_client:
            yield http_client
    finally:
        await app.router.shutdown()
