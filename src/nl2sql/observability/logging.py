from __future__ import annotations

import logging
import sys
import time
from typing import Any

import structlog

START_TIME = time.time()


def configure_logging(level: int = logging.INFO) -> None:
    logging.basicConfig(
        format="%(message)s",
        stream=sys.stdout,
        level=level,
    )

    structlog.configure(
        processors=[
            structlog.contextvars.merge_contextvars,
            structlog.processors.add_log_level,
            structlog.processors.TimeStamper(fmt="iso"),
            structlog.processors.JSONRenderer(),
        ],
        wrapper_class=structlog.make_filtering_bound_logger(level),
        cache_logger_on_first_use=True,
    )


def uptime_seconds() -> float:
    return time.time() - START_TIME


logger = structlog.get_logger("nl2sql")


def bind_run(run_id: str, **extra: Any) -> structlog.BoundLogger:
    return logger.bind(run_id=run_id, **extra)
