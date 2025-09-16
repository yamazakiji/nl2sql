from __future__ import annotations

from fastapi import APIRouter

from nl2sql.api.schemas import MetricsResponse
from nl2sql.metrics.store import metrics_store

router = APIRouter()


@router.get("/metrics", response_model=MetricsResponse)
async def get_metrics() -> MetricsResponse:
    return MetricsResponse(
        requests_total=metrics_store.requests_total,
        successful_requests=metrics_store.successful_requests,
        failed_requests=metrics_store.failed_requests,
    )
