from __future__ import annotations

import uuid

from arq import create_pool
from arq.connections import RedisSettings

from nl2sql.config import get_settings
from nl2sql.observability import logger


async def enqueue(job_name: str, **kwargs) -> str:
    settings = get_settings()
    try:
        pool = await create_pool(RedisSettings.from_dsn(settings.redis_url))
        job = await pool.enqueue_job(job_name, **kwargs)
        logger.info("jobs.enqueued", job=job_name, job_id=job.job_id)
        return str(job.job_id)
    except Exception as exc:  # pragma: no cover - fallback path
        job_id = f"local-{uuid.uuid4()}"
        logger.warning(
            "jobs.enqueue_fallback",
            job=job_name,
            job_id=job_id,
            error=str(exc),
        )
        return job_id
