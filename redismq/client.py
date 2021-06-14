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

    redis: Any
    namespace: str

    log_debug: Callable[..., None]

    def __init__(self) -> None:
        """
        default constructor - use connect() instead
        """
        Client.log_debug("__init__")

        self.redis = None
        self.namespace = "rmq"

    @classmethod
    async def connect(cls, address: str, namespace: Optional[str] = None) -> "Client":
        """
        Call to create a connected client.
        """
        Client.log_debug("connect %s", address)

        client = cls()
        client.redis = await aioredis.create_redis_pool(address, encoding="utf-8")
        if namespace:
            client.namespace = namespace

        return client

    async def producer(self, stream_name: str) -> Producer:
        """
        Use this to get a Producer
        """
        Client.log_debug("producer %s", stream_name)

        return Producer(self, stream_name)

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
