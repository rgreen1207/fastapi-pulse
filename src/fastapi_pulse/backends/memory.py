from __future__ import annotations

import asyncio
from typing import AsyncIterator

from .base import BaseBroker

_SENTINEL = object()


class MemoryBroker(BaseBroker):
    """In-process broker backed by asyncio.Queue.

    Suitable for testing and single-process deployments. Messages published
    before any subscriber is registered are silently dropped.
    """

    def __init__(self) -> None:
        # channel -> list of subscriber queues
        self._subscribers: dict[str, list[asyncio.Queue]] = {}
        self._closed = False

    async def publish(self, channel: str, message: str) -> None:
        if self._closed:
            return
        for q in list(self._subscribers.get(channel, [])):
            await q.put(message)

    async def subscribe(self, channel: str) -> AsyncIterator[str]:  # type: ignore[override]
        if self._closed:
            return
        queue: asyncio.Queue = asyncio.Queue()
        self._subscribers.setdefault(channel, []).append(queue)
        try:
            while True:
                item = await queue.get()
                if item is _SENTINEL:
                    break
                yield item
        finally:
            subs = self._subscribers.get(channel, [])
            if queue in subs:
                subs.remove(queue)
            if not subs:
                self._subscribers.pop(channel, None)

    async def close(self) -> None:
        self._closed = True
        for queues in self._subscribers.values():
            for q in queues:
                await q.put(_SENTINEL)
        self._subscribers.clear()
