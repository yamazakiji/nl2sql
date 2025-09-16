from __future__ import annotations

from nl2sql.config import get_settings

from .logging import configure_logging, logger, uptime_seconds
from .log_stream import LogStreamManager

log_manager = LogStreamManager(get_settings().sse_log_retention)

__all__ = [
    "configure_logging",
    "logger",
    "uptime_seconds",
    "log_manager",
]
