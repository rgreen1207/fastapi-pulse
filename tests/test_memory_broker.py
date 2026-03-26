import asyncio
import pytest
from fastapi_pulse.backends.memory import MemoryBroker


@pytest.mark.asyncio
async def test_publish_subscribe_single_message():
    broker = MemoryBroker()
    received = []

    async def collect():
        async for msg in broker.subscribe("chan:1"):
            received.append(msg)
            break

    task = asyncio.create_task(collect())
    await asyncio.sleep(0)
    await broker.publish("chan:1", "hello")
    await task

    assert received == ["hello"]
    await broker.close()


@pytest.mark.asyncio
async def test_publish_subscribe_multiple_subscribers():
    broker = MemoryBroker()
    results_a: list[str] = []
    results_b: list[str] = []

    async def sub_a():
        async for msg in broker.subscribe("chan:x"):
            results_a.append(msg)
            break

    async def sub_b():
        async for msg in broker.subscribe("chan:x"):
            results_b.append(msg)
            break

    t1 = asyncio.create_task(sub_a())
    t2 = asyncio.create_task(sub_b())
    await asyncio.sleep(0)
    await broker.publish("chan:x", "broadcast")
    await asyncio.gather(t1, t2)

    assert results_a == ["broadcast"]
    assert results_b == ["broadcast"]
    await broker.close()


@pytest.mark.asyncio
async def test_close_terminates_subscribers():
    broker = MemoryBroker()
    collected: list[str] = []

    async def collect():
        async for msg in broker.subscribe("chan:close"):
            collected.append(msg)

    task = asyncio.create_task(collect())
    await asyncio.sleep(0)
    await broker.close()
    await asyncio.wait_for(task, timeout=1.0)

    assert collected == []


@pytest.mark.asyncio
async def test_publish_with_no_subscribers_is_noop():
    broker = MemoryBroker()
    await broker.publish("chan:nobody", "msg")
    await broker.close()


@pytest.mark.asyncio
async def test_multiple_messages_in_order():
    broker = MemoryBroker()
    received: list[str] = []

    async def collect():
        async for msg in broker.subscribe("ordered"):
            received.append(msg)
            if len(received) == 3:
                break

    task = asyncio.create_task(collect())
    await asyncio.sleep(0)
    for i in range(3):
        await broker.publish("ordered", f"msg-{i}")
    await task

    assert received == ["msg-0", "msg-1", "msg-2"]
    await broker.close()


@pytest.mark.asyncio
async def test_close_is_idempotent():
    broker = MemoryBroker()
    await broker.close()
    await broker.close()  # must not raise
