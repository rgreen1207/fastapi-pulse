from __future__ import annotations

import json
from dataclasses import dataclass
from typing import Any


@dataclass
class TaskEvent:
    task_id: str
    event_type: str  # started | success | failure | retry | revoked
    task_name: str
    timestamp: str
    result: Any = None
    error: str | None = None
    traceback: str | None = None
    retries: int = 0

    def to_json(self) -> str:
        data: dict[str, Any] = {
            "task_id": self.task_id,
            "event": self.event_type,
            "task": self.task_name,
            "timestamp": self.timestamp,
        }
        if self.result is not None:
            data["result"] = self.result
        if self.error is not None:
            data["error"] = self.error
        if self.traceback is not None:
            data["traceback"] = self.traceback
        if self.retries:
            data["retries"] = self.retries
        return json.dumps(data)
