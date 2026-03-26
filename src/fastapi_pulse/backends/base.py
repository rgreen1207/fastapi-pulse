from __future__ import annotations

from abc import ABC, abstractmethod
from typing import AsyncIterator


class BaseBroker(ABC):
    """Abstract base class for fastapi-pulse broker backends.

    Implement :meth:`publish`, :meth:`subscribe`, and :meth:`close` to
    integrate any message transport (Kafka, SQS, NATS, etc.).

    :meth:`subscribe` must be an **async generator** that yields raw JSON
    strings indefinitely until the subscription is cancelled or the broker
    is closed.
    """

    @abstractmethod
    async def publish(self, channel: str, message: str) -> None:
        """Publish *message* to *channel*."""

    @abstractmethod
    async def subscribe(self, channel: str) -> AsyncIterator[str]:
        """Yield messages from *channel* as they arrive."""

    @abstractmethod
    async def close(self) -> None:
        """Release all connections and resources."""

    async def __aenter__(self) -> "BaseBroker":
        return self

    async def __aexit__(self, *args: object) -> None:
        await self.close()
