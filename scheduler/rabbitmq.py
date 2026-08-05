from __future__ import annotations

import asyncio
import json
from typing import Any

from core.conf import settings


class RabbitMqClient:
    """供消息发布者和消费者共用的轻量异步 RabbitMQ 客户端。"""

    def __init__(self) -> None:
        self._connection: Any = None
        self._channel: Any = None
        self._exchange: Any = None
        self._queue: Any = None
        self._lock = asyncio.Lock()

    async def _ensure_topology(self) -> None:
        if (
            self._connection is not None
            and not self._connection.is_closed
            and self._channel is not None
            and not self._channel.is_closed
        ):
            return

        async with self._lock:
            if (
                self._connection is not None
                and not self._connection.is_closed
                and self._channel is not None
                and not self._channel.is_closed
            ):
                return

            try:
                import aio_pika
            except ImportError as exc:
                raise RuntimeError("使用 RabbitMQ 必须安装 aio-pika 依赖") from exc

            self._connection = await aio_pika.connect_robust(settings.rabbitmq_url)
            self._channel = await self._connection.channel(publisher_confirms=True)
            await self._channel.set_qos(prefetch_count=settings.rabbitmq_prefetch_count)

            self._exchange = await self._channel.declare_exchange(
                settings.rabbitmq_exchange,
                aio_pika.ExchangeType.DIRECT,
                durable=True,
            )
            dead_exchange_name = f"{settings.rabbitmq_exchange}.dlx"
            dead_exchange = await self._channel.declare_exchange(
                dead_exchange_name,
                aio_pika.ExchangeType.DIRECT,
                durable=True,
            )
            dead_queue = await self._channel.declare_queue(
                settings.rabbitmq_dead_letter_queue,
                durable=True,
            )
            await dead_queue.bind(
                dead_exchange,
                routing_key=settings.rabbitmq_dead_letter_routing_key,
            )

            self._queue = await self._channel.declare_queue(
                settings.rabbitmq_queue,
                durable=True,
                arguments={
                    "x-dead-letter-exchange": dead_exchange_name,
                    "x-dead-letter-routing-key": settings.rabbitmq_dead_letter_routing_key,
                },
            )
            await self._queue.bind(
                self._exchange,
                routing_key=settings.rabbitmq_routing_key,
            )

    async def publish(self, payload: dict[str, Any], attempt: int = 0) -> None:
        await self._ensure_topology()
        import aio_pika

        body = json.dumps(payload, ensure_ascii=False).encode("utf-8")
        dispatch_key = str(payload["dispatchKey"])
        message = aio_pika.Message(
            body=body,
            content_type="application/json",
            delivery_mode=aio_pika.DeliveryMode.PERSISTENT,
            message_id=dispatch_key,
            headers={"x-attempt": attempt},
        )
        await self._exchange.publish(message, routing_key=settings.rabbitmq_routing_key)

    async def get_queue(self) -> Any:
        await self._ensure_topology()
        return self._queue

    async def close(self) -> None:
        if self._connection is not None and not self._connection.is_closed:
            await self._connection.close()
        self._connection = None
        self._channel = None
        self._exchange = None
        self._queue = None


_client: RabbitMqClient | None = None


def get_rabbitmq() -> RabbitMqClient:
    global _client
    if _client is None:
        _client = RabbitMqClient()
    return _client


async def publish_rmf_dispatch(payload: dict[str, Any], attempt: int = 0) -> None:
    await get_rabbitmq().publish(payload, attempt=attempt)


async def close_rabbitmq() -> None:
    if _client is not None:
        await _client.close()
