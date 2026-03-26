"""Tests for TaskBridge using MemoryBroker (no external dependencies)."""
import asyncio
import json
import pytest
from unittest.mock import MagicMock
from fastapi import FastAPI
from fastapi_pulse import TaskBridge
from fastapi_pulse.backends.memory import MemoryBroker
from fastapi_pulse.events import TaskEvent


def make_app():
    app = FastAPI()
    broker = MemoryBroker()
    celery_app = MagicMock()
    bridge = TaskBridge(app, celery_app, broker=broker)
    return app, bridge, broker


def test_sse_route_registered():
    app, bridge, broker = make_app()
    paths = {r.path for r in app.routes}
    assert "/pulse/tasks/{task_id}" in paths


def test_custom_route_prefix():
    app = FastAPI()
    broker = MemoryBroker()
    celery_app = MagicMock()
    TaskBridge(app, celery_app, broker=broker, route_prefix="/events")
    paths = {r.path for r in app.routes}
    assert "/events/tasks/{task_id}" in paths


@pytest.mark.asyncio
async def test_stream_receives_published_event():
    app, bridge, broker = make_app()
    received: list[str] = []

    async def collect():
        async for msg in broker.subscribe("task:test-id-1"):
            received.append(msg)
            break

    task = asyncio.create_task(collect())
    await asyncio.sleep(0)
    await broker.publish(
        "task:test-id-1",
        json.dumps({"event": "success", "task_id": "test-id-1"}),
    )
    await asyncio.wait_for(task, timeout=2.0)

    assert len(received) == 1
    data = json.loads(received[0])
    assert data["event"] == "success"
    await broker.close()


def test_task_event_success_to_json():
    event = TaskEvent(
        task_id="abc",
        event_type="success",
        task_name="my.task",
        timestamp="2024-01-01T00:00:00+00:00",
        result=42,
    )
    data = json.loads(event.to_json())
    assert data["task_id"] == "abc"
    assert data["event"] == "success"
    assert data["result"] == 42
    assert "error" not in data


def test_task_event_failure_to_json():
    event = TaskEvent(
        task_id="abc",
        event_type="failure",
        task_name="my.task",
        timestamp="2024-01-01T00:00:00+00:00",
        error="ValueError: bad input",
        traceback="Traceback ...",
    )
    data = json.loads(event.to_json())
    assert data["error"] == "ValueError: bad input"
    assert data["traceback"] == "Traceback ..."
    assert "result" not in data


def test_task_event_retry_includes_retries():
    event = TaskEvent(
        task_id="abc",
        event_type="retry",
        task_name="my.task",
        timestamp="2024-01-01T00:00:00+00:00",
        error="temporary failure",
        retries=2,
    )
    data = json.loads(event.to_json())
    assert data["retries"] == 2


def test_task_event_no_retries_field_when_zero():
    event = TaskEvent(
        task_id="abc",
        event_type="started",
        task_name="my.task",
        timestamp="2024-01-01T00:00:00+00:00",
    )
    data = json.loads(event.to_json())
    assert "retries" not in data
