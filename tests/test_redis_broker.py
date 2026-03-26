"""Integration tests for RedisBroker — skipped if aioredis is not installed or Redis unavailable."""
import asyncio
import pytest

try:
    import aioredis
    HAS_AIOREDIS = True
except ImportError:
    HAS_AIOREDIS = False

pytestmark = pytest.mark.skipif(not HAS_AIOREDIS, reason="aioredis not installed")

REDIS_URL = "redis://localhost:6379"


async def _redis_available() -> bool:
    try:
        import aioredis
        r = await aioredis.from_url(REDIS_URL)
        await r.ping()
        await r.aclose()
        return True
    except Exception:
        return False


@pytest.fixture
async def redis_broker():
    if not await _redis_available():
        pytest.skip("Redis not available")
    from fastapi_pulse.backends.redis import RedisBroker
    broker = RedisBroker(url=REDIS_URL)
    yield broker
    await broker.close()


@pytest.mark.asyncio
async def test_redis_publish_subscribe(redis_broker):
    received: list[str] = []

    async def collect():
        async for msg in redis_broker.subscribe("test:pulse:chan"):
            received.append(msg)
            break

    task = asyncio.create_task(collect())
    await asyncio.sleep(0.2)  # allow pubsub registration
    await redis_broker.publish("test:pulse:chan", "redis-hello")
    await asyncio.wait_for(task, timeout=5.0)

    assert received == ["redis-hello"]


@pytest.mark.asyncio
async def test_redis_broker_close_idempotent(redis_broker):
    await redis_broker.close()
    await redis_broker.close()  # must not raise
