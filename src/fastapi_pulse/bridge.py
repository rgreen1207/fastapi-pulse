from __future__ import annotations

import asyncio
import json
import logging
from contextlib import asynccontextmanager
from datetime import datetime, timezone
from typing import Any

from fastapi import FastAPI
from fastapi.responses import StreamingResponse
from celery import Celery
from celery.signals import (
    task_failure,
    task_prerun,
    task_retry,
    task_revoked,
    task_success,
)

from .backends.base import BaseBroker
from .events import TaskEvent

logger = logging.getLogger(__name__)


def _now() -> str:
    return datetime.now(timezone.utc).isoformat()


def _safe_json(value: Any) -> Any:
    """Return *value* if JSON-serialisable, otherwise its string representation."""
    try:
        json.dumps(value)
        return value
    except (TypeError, ValueError):
        return str(value)


class TaskBridge:
    """Connect Celery task signals to FastAPI SSE endpoints.

    Args:
        app: The FastAPI application instance.
        celery_app: The Celery application instance.
        broker: A :class:`~fastapi_pulse.backends.base.BaseBroker` implementation.
        route_prefix: URL prefix for the SSE route (default ``/pulse``).

    The bridge mounts ``GET {route_prefix}/tasks/{task_id}`` on *app* and
    registers Celery signal handlers that publish lifecycle events to the
    broker channel ``task:{task_id}``.

    Example::

        bridge = TaskBridge(app, celery_app, broker=RedisBroker("redis://localhost"))
    """

    def __init__(
        self,
        app: FastAPI,
        celery_app: Celery,
        broker: BaseBroker,
        route_prefix: str = "/pulse",
    ) -> None:
        self.app = app
        self.celery_app = celery_app
        self.broker = broker
        self.route_prefix = route_prefix

        self._register_signals()
        self._register_routes()
        self._attach_shutdown_hook()

    # ------------------------------------------------------------------
    # Internals
    # ------------------------------------------------------------------

    def _publish_sync(self, channel: str, message: str) -> None:
        """Fire-and-forget publish from synchronous Celery signal handlers."""
        try:
            loop = asyncio.get_running_loop()
            # We're inside an already-running loop (e.g. async worker).
            asyncio.ensure_future(self.broker.publish(channel, message), loop=loop)
        except RuntimeError:
            # No running loop — typical in a Celery worker process.
            try:
                asyncio.run(self.broker.publish(channel, message))
            except Exception:
                logger.exception("fastapi-pulse: failed to publish to %s", channel)

    def _attach_shutdown_hook(self) -> None:
        """Close the broker when the FastAPI app shuts down."""
        original_lifespan = self.app.router.lifespan_context

        @asynccontextmanager
        async def _lifespan(app: FastAPI):  # type: ignore[misc]
            async with original_lifespan(app):
                try:
                    yield
                finally:
                    await self.broker.close()

        self.app.router.lifespan_context = _lifespan

    # ------------------------------------------------------------------
    # Celery signal handlers
    # ------------------------------------------------------------------

    def _register_signals(self) -> None:
        @task_prerun.connect(weak=False)
        def _on_prerun(task_id: str, task: Any, *args: Any, **kwargs: Any) -> None:
            event = TaskEvent(
                task_id=task_id,
                event_type="started",
                task_name=task.name,
                timestamp=_now(),
            )
            self._publish_sync(f"task:{task_id}", event.to_json())

        @task_success.connect(weak=False)
        def _on_success(sender: Any, result: Any, **kwargs: Any) -> None:
            task_id: str = sender.request.id
            event = TaskEvent(
                task_id=task_id,
                event_type="success",
                task_name=sender.name,
                timestamp=_now(),
                result=_safe_json(result),
            )
            self._publish_sync(f"task:{task_id}", event.to_json())

        @task_failure.connect(weak=False)
        def _on_failure(
            sender: Any,
            task_id: str,
            exception: Exception,
            traceback: Any,
            **kwargs: Any,
        ) -> None:
            event = TaskEvent(
                task_id=task_id,
                event_type="failure",
                task_name=sender.name,
                timestamp=_now(),
                error=str(exception),
                traceback=str(traceback) if traceback else None,
            )
            self._publish_sync(f"task:{task_id}", event.to_json())

        @task_retry.connect(weak=False)
        def _on_retry(
            sender: Any,
            request: Any,
            reason: Any,
            einfo: Any,
            **kwargs: Any,
        ) -> None:
            event = TaskEvent(
                task_id=request.id,
                event_type="retry",
                task_name=sender.name,
                timestamp=_now(),
                error=str(reason),
                retries=request.retries,
            )
            self._publish_sync(f"task:{request.id}", event.to_json())

        @task_revoked.connect(weak=False)
        def _on_revoked(
            sender: Any,
            request: Any,
            terminated: bool,
            signum: Any,
            expired: bool,
            **kwargs: Any,
        ) -> None:
            task_id = getattr(request, "id", str(request))
            task_name = getattr(request, "task", "unknown")
            event = TaskEvent(
                task_id=task_id,
                event_type="revoked",
                task_name=task_name,
                timestamp=_now(),
            )
            self._publish_sync(f"task:{task_id}", event.to_json())

    # ------------------------------------------------------------------
    # FastAPI routes
    # ------------------------------------------------------------------

    def _register_routes(self) -> None:
        broker = self.broker
        prefix = self.route_prefix

        @self.app.get(
            f"{prefix}/tasks/{{task_id}}",
            response_class=StreamingResponse,
            summary="Stream task lifecycle events",
            description=(
                "Opens a Server-Sent Events stream for a single Celery task. "
                "Each ``data`` field is a JSON object containing: "
                "``task_id``, ``event``, ``task``, ``timestamp``, and optionally "
                "``result``, ``error``, ``traceback``, ``retries``."
            ),
            tags=["pulse"],
        )
        async def stream_task_events(task_id: str) -> StreamingResponse:
            async def _generator():
                async for message in broker.subscribe(f"task:{task_id}"):
                    yield f"data: {message}\n\n"

            return StreamingResponse(
                _generator(),
                media_type="text/event-stream",
                headers={
                    "Cache-Control": "no-cache",
                    "Connection": "keep-alive",
                    "X-Accel-Buffering": "no",
                },
            )
