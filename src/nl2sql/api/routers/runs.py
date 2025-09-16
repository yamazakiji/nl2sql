from __future__ import annotations

from fastapi import APIRouter
from sse_starlette.sse import EventSourceResponse

from nl2sql.observability import log_manager

router = APIRouter()


@router.get("/runs/{run_id}/logs/stream")
async def stream_logs(run_id: str) -> EventSourceResponse:
    async def event_publisher():
        async for line in log_manager.stream(run_id):
            yield {
                "event": "message",
                "data": line,
            }

    return EventSourceResponse(event_publisher())
