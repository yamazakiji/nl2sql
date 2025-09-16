from __future__ import annotations

import threading
from contextlib import contextmanager
from typing import Iterator


class MetricsStore:
    def __init__(self) -> None:
        self._lock = threading.Lock()
        self.requests_total = 0
        self.successful_requests = 0
        self.failed_requests = 0

    @contextmanager
    def track(self) -> Iterator[None]:
        self.increment_requests()
        try:
            yield
        except Exception:
            self.increment_failed()
            raise
        else:
            self.increment_success()

    def increment_requests(self) -> None:
        with self._lock:
            self.requests_total += 1

    def increment_success(self) -> None:
        with self._lock:
            self.successful_requests += 1

    def increment_failed(self) -> None:
        with self._lock:
            self.failed_requests += 1


metrics_store = MetricsStore()
