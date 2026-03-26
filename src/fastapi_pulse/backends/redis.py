from __future__ import annotations

from typing import TYPE_CHECKING, AsyncIterator

from .base import BaseBroker

if TYPE_CHECKING:
    import aioredis


class RedisBroker(BaseBroker):
    """Broker backend using Redis Pub/Sub via aioredis.

    Install with: ``pip install fastapi-pulse[redis]``
    """

    def __init__(self, url: str = "redis://localhost") -> None:
        self.url = url
        self._redis: "aioredis.Redis | None" = None

    async def _get_redis(self) -> "aioredis.Redis":
        if self._redis is None:
            try:
                import aioredis
            except ImportError as exc:
                raise ImportError(
                    "Install fastapi-pulse[redis] to use RedisBroker."
                ) from exc
            self._redis = await aioredis.from_url(self.url, decode_responses=True)
        return self._redis

    async def publish(self, channel: str, message: str) -> None:
        redis = await self._get_redis()
        await redis.publish(channel, message)

    async def subscribe(self, channel: str) -> AsyncIterator[str]:  # type: ignore[override]
        try:
            import aioredis
        except ImportError as exc:
            raise ImportError(
                "Install fastapi-pulse[redis] to use RedisBroker."
            ) from exc
        # Each subscriber gets its own connection so publishes don't block.
        redis = await aioredis.from_url(self.url, decode_responses=True)
        pubsub = redis.pubsub()
        await pubsub.subscribe(channel)
        try:
            async for msg in pubsub.listen():
                if msg["type"] == "message":
                    yield msg["data"]
        finally:
            await pubsub.unsubscribe(channel)
            await pubsub.aclose()
            await redis.aclose()

    async def close(self) -> None:
        if self._redis is not None:
            await self._redis.aclose()
            self._redis = None
