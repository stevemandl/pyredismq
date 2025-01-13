"""
Client for RedisMQ
"""

from __future__ import annotations

import asyncio
from redis import asyncio as aioredis  # type: ignore[attr-defined]

from typing import Any, Callable, Dict, Set, Optional

from .debugging import debugging
from .producer import Producer
from .consumer import Consumer

__all__ = ["Client"]


# producer default settings
MAXLEN = 100
TIMEOUT = 10.0


@debugging
class Client:
    """
    Top level class to interface with message queue
    """

    log_debug: Callable[..., None]

    status: str
    namespace: str
    redis: Any
    pubsub: Any
    sub_task: Any

    payloads: Set[Any]
    payloads_updated: asyncio.Condition

    producer_registry: Dict[str, Producer]

    def __init__(self) -> None:
        """
        default constructor - use connect() instead
        """
        Client.log_debug("__init__")

        self.namespace = "rmq"
        self.producer_registry = {}
        self.status = "wait"

        # keep track of the un-acked payloads
        self.payloads = set()
        self.payloads_event = asyncio.Event()
        self.payloads_event.set()

    @classmethod
    async def connect(cls, address: str, namespace: Optional[str] = None) -> "Client":
        """
        Call to create a connection pool.
        """
        Client.log_debug("connect %s", address)

        # create a Client instance or one of its subclasses
        client = cls()

        # set the namespace
        if namespace:
            cls.namespace = namespace

        # we are 'connecting' but not really until the PING
        client.status = "connecting"

        # create a blocking connection pool to wait for a connection to become available
        # rather than raising an exception
        # see https://aioredis.readthedocs.io/en/latest/api/low-level/#aioredis.connection.BlockingConnectionPool
        pool = aioredis.BlockingConnectionPool.from_url(
            address, max_connections=10, decode_responses=True
        )
        client.redis = aioredis.Redis(connection_pool=pool)
        Client.log_debug("    - redis: %s", client.redis)

        # try to ping it
        rslt = await client.redis.ping()
        Client.log_debug("    - ping: %r", rslt)

        client.pubsub = client.redis.pubsub(ignore_subscribe_messages=True)
        client.sub_task = asyncio.get_running_loop().create_task(client.pubsub.run())
        client.status = "ready"

        return client

    async def close(self) -> None:
        """
        Call to wait for all of the active payloads to complete.
        """
        Client.log_debug("close")
        if self.status in ["closed", "closing"]:
            raise RuntimeError("Client.close() has already been called")
        if self.status != "ready":
            raise RuntimeError("Client is not ready to close")

        self.status = "closing"

        # wait for the event that says no more pending
        Client.log_debug(f"    - payloads: {self.payloads}")
        await self.payloads_event.wait()
        self.sub_task.cancel()
        try:
            await self.sub_task
        except asyncio.CancelledError:
            Client.log_debug(f"    - sub_task cancelled")
        await self.pubsub.aclose()
        Client.log_debug(f"    - pubsub closed")
        await self.redis.aclose()
        Client.log_debug(f"    - redis closed")
        await self.redis.connection_pool.disconnect()
        Client.log_debug(f"    - connection_pool disconnected")

        self.status = "closed"

    async def producer(
        self, stream_name: str, maxlen: int = MAXLEN, timeout: float = TIMEOUT
    ) -> Producer:
        """
        Use this to get a Producer
        """
        Client.log_debug("producer %s", stream_name)
        if stream_name not in self.producer_registry:
            Client.log_debug("    - adding producer %s to registry", stream_name)
            self.producer_registry[stream_name] = Producer(
                self, stream_name, maxlen, timeout
            )
        else:
            Client.log_debug("    - producer %s found in registry", stream_name)
        return self.producer_registry[stream_name]

    async def dispose_producer(self, producer: Producer) -> None:
        """dispose_producer of client
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
        scan_pending_on_start: bool = True,
        claim_stale_messages: bool = True,
        min_idle_time: int = 60000,
    ) -> Consumer:
        """
        Use this to get a Consumer
        """
        Client.log_debug("consumer %s ...", stream_name)

        try:
            group_info = await self.redis.xinfo_groups(stream_name)
            Client.log_debug("    - xinfo_groups: %r", group_info)
        except aioredis.RedisError as err:
            Client.log_debug("    - no existing group: %r", err)
            group_info = []

        if not any(e["name"] == group_name for e in group_info):
            await self.redis.xgroup_create(
                stream_name, group_name, id="$", mkstream=True
            )
            Client.log_debug(
                "    - added group %s to stream %s ", group_name, stream_name
            )

        # create a consumer
        consumer = Consumer(
            self,
            stream_name,
            group_name,
            consumer_id,
            min_idle_time,
        )

        try:
            stream_info = await self.redis.xinfo_stream(stream_name)
            Client.log_debug("    - xinfo_stream: %r", stream_info)

            # set the consumer to read new messages that haven't
            # been delivered to another consumer
            consumer.latest_id = b">"

        except aioredis.RedisError as err:
            Client.log_debug("    - no existing stream: %r", err)
            stream_info = []

        if scan_pending_on_start:
            rslt = await self.redis.xpending(stream_name, group_name)
            Client.log_debug(f"    - xpending: %r", rslt)

            pending_count = rslt["pending"]
            min_id = rslt["min"]
            max_id = rslt["max"]
            pending_consumers = rslt["consumers"]
            if not pending_count:
                Client.log_debug("    - no pending messages")

            for pending_info in pending_consumers:
                pending_consumer = pending_info["name"]
                pending_consumer_count = pending_info["pending"]
                if pending_consumer == consumer_id:
                    Client.log_debug(
                        f"    - this consumer has {pending_consumer_count} pending messages"
                    )
                else:
                    Client.log_debug(f"    - pending messages for {pending_consumer!r}")

                pending_messages = await self.redis.xpending_range(
                    stream_name,
                    group_name,
                    min=b"-",
                    max=b"+",
                    count=pending_consumer_count,
                    consumername=pending_consumer,
                )
                Client.log_debug(f"    - pending_messages: %r", pending_messages)

                for pending_message_info in pending_messages:
                    message_id = pending_message_info["message_id"]
                    pending_consumer = pending_message_info["consumer"]
                    time_since_delivered = pending_message_info["time_since_delivered"]
                    times_delivered = pending_message_info["times_delivered"]

                    Client.log_debug(
                        "        %r for %r, idle %rs, delivered %r times",
                        message_id,
                        pending_consumer,
                        time_since_delivered / 1000.0,
                        times_delivered,
                    )
                    if claim_stale_messages:
                        retcode = await self.redis.xclaim(
                            stream_name,
                            group_name,
                            consumer_id,
                            min_idle_time,
                            [message_id],
                        )
                        Client.log_debug(f"        claim: {retcode!r}")

                        # start at the beginning of the pending messages
                        consumer.latest_id = b"0-0"
                        consumer.check_backlog = True

        return consumer
