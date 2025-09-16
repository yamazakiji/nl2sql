from __future__ import annotations

from arq.connections import RedisSettings

from nl2sql.config import get_settings

from .tasks import schema_snapshot_job, training_run_job


_settings = get_settings()


class WorkerSettings:
    functions = [schema_snapshot_job, training_run_job]
    redis_settings = RedisSettings.from_dsn(_settings.redis_url)
