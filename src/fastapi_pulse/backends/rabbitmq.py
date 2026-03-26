from __future__ import annotations

from typing import TYPE_CHECKING, AsyncIterator

from .base import BaseBroker

if TYPE_CHECKING:
    import aio_pika

_EXCHANGE_NAME = "fastapi_pulse"


class RabbitMQBroker(BaseBroker):
    """Broker backend using a RabbitMQ topic exchange via aio-pika.

    Install with: ``pip install fastapi-pulse[rabbitmq]``

    Each call to :meth:`subscribe` opens a dedicated connection with an
    exclusive, auto-delete queue bound to the given routing key (channel),
    so multiple concurrent subscribers on the same channel each receive
    every message.
    """

    def __init__(self, url: str = "amqp://guest:guest@localhost/") -> None:
        self.url = url
        self._connection: "aio_pika.abc.AbstractRobustConnection | None" = None
        self._channel: "aio_pika.abc.AbstractChannel | None" = None
        self._exchange: "aio_pika.abc.AbstractExchange | None" = None

    async def _get_exchange(self) -> "aio_pika.abc.AbstractExchange":
        try:
            import aio_pika
        except ImportError as exc:
            raise ImportError(
                "Install fastapi-pulse[rabbitmq] to use RabbitMQBroker."
            ) from exc
        if self._connection is None:
            self._connection = await aio_pika.connect_robust(self.url)
            self._channel = await self._connection.channel()
            self._exchange = await self._channel.declare_exchange(
                _EXCHANGE_NAME,
                aio_pika.ExchangeType.TOPIC,
                durable=True,
            )
        return self._exchange  # type: ignore[return-value]

    async def publish(self, channel: str, message: str) -> None:
        try:
            import aio_pika
        except ImportError as exc:
            raise ImportError(
                "Install fastapi-pulse[rabbitmq] to use RabbitMQBroker."
            ) from exc
        exchange = await self._get_exchange()
        await exchange.publish(
            aio_pika.Message(body=message.encode()),
            routing_key=channel,
        )

    async def subscribe(self, channel: str) -> AsyncIterator[str]:  # type: ignore[override]
        try:
            import aio_pika
        except ImportError as exc:
            raise ImportError(
                "Install fastapi-pulse[rabbitmq] to use RabbitMQBroker."
            ) from exc
        connection = await aio_pika.connect_robust(self.url)
        ch = await connection.channel()
        exchange = await ch.declare_exchange(
            _EXCHANGE_NAME,
            aio_pika.ExchangeType.TOPIC,
            durable=True,
        )
        queue = await ch.declare_queue(exclusive=True, auto_delete=True)
        await queue.bind(exchange, routing_key=channel)
        try:
            async with queue.iterator() as it:
                async for message in it:
                    async with message.process():
                        yield message.body.decode()
        finally:
            await ch.close()
            await connection.close()

    async def close(self) -> None:
        if self._channel is not None:
            await self._channel.close()
        if self._connection is not None:
            await self._connection.close()
        self._connection = None
        self._channel = None
        self._exchange = None
