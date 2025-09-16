from __future__ import annotations

import platform
from datetime import UTC, datetime

from fastapi import APIRouter

from nl2sql import __version__
from nl2sql.config import get_settings
from nl2sql.observability import uptime_seconds

router = APIRouter()


@router.get("/health")
async def get_health() -> dict[str, object]:
    settings = get_settings()
    return {
        "status": "ok",
        "uptime_seconds": uptime_seconds(),
        "version": __version__,
        "python": platform.python_version(),
        "environment": settings.environment,
        "timestamp": datetime.now(UTC).isoformat(),
    }
