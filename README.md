# fastapi-pulse

Bridge [Celery](https://docs.celeryq.dev/) task lifecycle events to [FastAPI](https://fastapi.tiangolo.com/) Server-Sent Events (SSE) streams.

## Features

- Real-time task progress via SSE — no polling required
- Pluggable broker backends: Redis, RabbitMQ, or in-process Memory
- Single route per task: `GET /pulse/tasks/{task_id}`
- Bring your own broker by subclassing `BaseBroker`

## Installation

```bash
# Core + Redis backend
pip install fastapi-pulse[redis]

# Core + RabbitMQ backend
pip install fastapi-pulse[rabbitmq]

# All backends
pip install fastapi-pulse[all]
```

## Quick start

```python
from fastapi import FastAPI
from celery import Celery
from fastapi_pulse import TaskBridge
from fastapi_pulse.backends import RedisBroker

app = FastAPI()
celery_app = Celery("worker", broker="redis://localhost/0")

bridge = TaskBridge(app, celery_app, broker=RedisBroker(url="redis://localhost"))
```

Connect from a browser or client:

```js
const es = new EventSource("/pulse/tasks/YOUR_TASK_ID");
es.onmessage = (e) => console.log(JSON.parse(e.data));
```

## Backends

| Backend | Import | Extra |
|---------|--------|-------|
| Redis | `from fastapi_pulse.backends import RedisBroker` | `pip install fastapi-pulse[redis]` |
| RabbitMQ | `from fastapi_pulse.backends import RabbitMQBroker` | `pip install fastapi-pulse[rabbitmq]` |
| Memory | `from fastapi_pulse.backends import MemoryBroker` | built-in |

## Custom backend

```python
from fastapi_pulse.backends import BaseBroker
from typing import AsyncIterator

class MyBroker(BaseBroker):
    async def publish(self, channel: str, message: str) -> None: ...
    async def subscribe(self, channel: str) -> AsyncIterator[str]: ...
    async def close(self) -> None: ...
```

## Event format

Each SSE `data` field is a JSON object:

```json
{
  "task_id": "abc-123",
  "event": "success",
  "task": "myapp.tasks.add",
  "timestamp": "2024-01-01T00:00:00+00:00",
  "result": 42
}
```

`event` values: `started` | `success` | `failure` | `retry` | `revoked`

## License

MIT
