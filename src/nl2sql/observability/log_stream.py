from __future__ import annotations

import asyncio
from collections import defaultdict, deque
from typing import AsyncIterator, Deque, Dict, List


class LogStreamManager:
    def __init__(self, retention: int) -> None:
        self._retention = retention
        self._buffers: Dict[str, Deque[str]] = defaultdict(deque)
        self._subscribers: Dict[str, List[asyncio.Queue[str]]] = defaultdict(list)
        self._lock = asyncio.Lock()

    async def emit(self, run_id: str, message: str) -> None:
        async with self._lock:
            buffer = self._buffers[run_id]
            buffer.append(message)
            while len(buffer) > self._retention:
                buffer.popleft()
            subscribers = list(self._subscribers[run_id])
        for queue in subscribers:
            await queue.put(message)

    async def stream(self, run_id: str) -> AsyncIterator[str]:
        queue: asyncio.Queue[str] = asyncio.Queue()
        async with self._lock:
            buffer = list(self._buffers[run_id])
            self._subscribers[run_id].append(queue)
        try:
            for item in buffer:
                yield item
            while True:
                item = await queue.get()
                yield item
        finally:
            async with self._lock:
                subscribers = self._subscribers[run_id]
                if queue in subscribers:
                    subscribers.remove(queue)
                if not subscribers and not self._buffers[run_id]:
                    self._buffers.pop(run_id, None)
                    self._subscribers.pop(run_id, None)
