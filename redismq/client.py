"""
Client for RedisMQ
"""

from __future__ import annotations

import aioredis  # type: ignore

from typing import Any, Callable, Optional

from .debugging import debugging
from .producer import Producer
from .consumer import Consumer

__all__ = ["Client"]


@debugging
class Client:
    """
    Top level class to interface with message queue
    """

    log_debug: Callable[..., None]

    def __init__(self) -> None:
        """
        default constructor - use connect() instead
        """
        Client.log_debug("__init__")

        self.redis: Any = None
        self.namespace: str = "rmq"
        self.producer_registry: object = {}
        self.status: str = "wait"

    # this is a classmethod so it can be async
    @classmethod
    async def connect(cls, address: str, namespace: Optional[str] = None) -> "Client":
        """
        Call to create a connected client.
        """
        Client.log_debug("connect %s", address)
        client = cls()
        client.status = "connecting"
        client.redis = await aioredis.create_redis_pool(address, encoding="utf-8")
        if namespace:
            client.namespace = namespace
        client.status = "ready"
        return client

    async def close(self) -> None:
        """
        Call to close a connected client.
        """
        Client.log_debug("close")
        if self.status in ["closed", "closing"]:
            raise RuntimeError("Client.close() has already been called")
        if self.status != "ready":
            raise RuntimeError("Client is not ready to close")
        self.status = "closing"
        self.redis.close()
        await self.redis.wait_closed()
        self.status = "closed"

    async def producer(self, stream_name: str) -> Producer:
        """
        Use this to get a Producer
        """
        Client.log_debug("producer %s", stream_name)
        if stream_name not in self.producer_registry:
            Client.log_debug("    - adding producer %s to registry", stream_name)
            self.producer_registry[stream_name] = Producer(self, stream_name)
        else:
            Client.log_debug("    - producer %s found in registry", stream_name)
        return self.producer_registry[stream_name]

    async def dispose_producer(self, producer: Producer) -> None:
        """ dispose_producer of client
            @param producer - the producer to dispose of
        """
        stream_name = producer.stream_name
        if self.producer_registry[stream_name] is producer:
            del self.producer_registry[stream_name]
            producer.destroy()
            Client.log_debug("dispose_producer - producer %s disposed", stream_name)
        else:
            raise ValueError("Producer named %s registry entry mismatch." % stream_name)

    async def consumer(
        self,
        stream_name: str,
        group_name: str,
        consumer_id: str,
        # scan_pending_on_start: bool = True,
        # claim_stale_messages: bool = True,
        # min_idle_time: int = 60000,
    ) -> Consumer:
        """
        Use this to get a Consumer
        """
        Client.log_debug("consumer %s ...", stream_name)

        try:
            info = await self.redis.xinfo_groups(stream_name)
            Client.log_debug("    - xinfo_groups %s", info)
        except aioredis.RedisError as err:
            Client.log_debug("    - no existing stream %s", stream_name)
            info = []

        if not any(e["name"] == group_name for e in info):
            await self.redis.xgroup_create(
                stream_name, group_name, latest_id="$", mkstream=True
            )
            Client.log_debug(
                "    - added group %s to stream %s ", group_name, stream_name
            )

        return Consumer(
            self,
            stream_name,
            group_name,
            consumer_id,
            # scan_pending_on_start,
            # claim_stale_messages,
            # min_idle_time,
        )
