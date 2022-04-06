"""
Producer for RedisMQ
"""
from __future__ import annotations

import asyncio
import json
import time

from typing import TYPE_CHECKING, Any, Callable, Dict, TypedDict, Optional, cast
from redis import Connection # type: ignore[attr-defined]
from .debugging import debugging

Client = TypedDict('Client', redis=Connection)

# class is declared as generic in stubs but not at runtime
AnyFuture = asyncio.Future

# settings
MAXLEN = 100
TIMEOUT = 10.0


@debugging
class Producer:
    """
    Produces messages
    """

    client: Client
    stream_name: str
    channel_key: str
    maxlen: int
    timeout: float
    id: int

    log_debug: Callable[..., None]

    def __init__(
        self,
        client: Client,
        stream_name: str,
        maxlen: int = MAXLEN,
        timeout: float = TIMEOUT,
    ) -> None:
        """
        default constructor
        """
        Producer.log_debug("__init__ %r %r", client, stream_name)

        self.client = client
        self.stream_name = stream_name
        self.channel_key = "%s:responseid" % client.namespace
        self.maxlen = maxlen
        self.timeout = timeout

        self.id = time.time_ns()

    async def _resp_task(
        self, payload: Dict[str, Any], response_channel_id: Optional[str]
    ) -> Any:
        """
        utility method for a confirmed request
        """
        Producer.log_debug("_resp_task: %r %r", payload, response_channel_id)

        # get a unique channel identifier if one hasn't been provided
        if response_channel_id is None:
            uid = await self.client.redis.incr(self.channel_key)
            response_channel_id = "%s:response.%d" % (self.client.namespace, uid)
        Producer.log_debug("    - response_channel_id: %r", response_channel_id)

        # pack it into the request payload
        payload["response_channel"] = response_channel_id

        async with self.client.redis.pubsub() as pubsub:
            Producer.log_debug("    - pubsub: %r", pubsub)

            # start listening for a response
            await pubsub.subscribe(response_channel_id)
            Producer.log_debug("    - subscribed")

            # put the request into the stream
            message_id: str = await self.client.redis.xadd(
                self.stream_name, payload, maxlen=self.maxlen
            )
            Producer.log_debug("    - message_id: %r", message_id)

            try:
                # wait for the response to come back encoded as JSON
                while True:
                    json_message = await pubsub.get_message(
                        ignore_subscribe_messages=True, timeout=self.timeout
                    )
                    Producer.log_debug("    - json_message: %r", json_message)
                    if json_message:
                        response = json.loads(json_message["data"])
                        break
            except ValueError as err:
                Producer.log_debug("    - value/decoding error %s", response_channel_id)
                response = {"message": "JSON Decoding Error", "err": err}
            except asyncio.CancelledError as err:
                Producer.log_debug("    - canceled %s", response_channel_id)
                response = {"message": "Cancelled Error", "err": err}
            finally:
                Producer.log_debug("    - finally %s", response_channel_id)
                await pubsub.unsubscribe(response_channel_id)

        return response

    # pylint: disable=invalid-name
    def addUnconfirmedMessage(
        self, message: Any, response_channel_id: str = None
    ) -> AnyFuture:
        """
        Return a task that adds an unconfirmed message to the message queue.
        """
        Producer.log_debug("addUnconfirmedMessage %r", message)

        # JSON encode the message
        payload = {"message": json.dumps(message)}
        if response_channel_id is not None:
            payload["response_channel"] = response_channel_id

        # create a task to add it to the stream
        future = self.client.redis.xadd(
            self.stream_name, payload, maxlen=self.maxlen
        )

        return cast(AnyFuture, future)

    # pylint: disable=invalid-name
    def addConfirmedMessage(
        self, message: Any, response_channel_id: Optional[str] = None
    ) -> AnyFuture:
        """
        Return a task that adds a confirmed message to the message queue and
        waits for the response.
        """
        Producer.log_debug("addConfirmedMessage %r", message)

        # JSON encode the message
        payload = {"message": json.dumps(message)}

        # create a task to add it to the stream
        future = self._resp_task(payload, response_channel_id)

        return cast(AnyFuture, future)

    # pylint: disable=invalid-name
    def destroy(self) -> None:
        """
        Stops this producer from working. This is automatically called when
        client.dispose_producer() is called with this producer.
        """
        Producer.log_debug("destroy")

        # assume this hasn't been gracefully closed
        # close_task = asyncio.create_task(self.close())
        # Producer.log_debug("    - close task: %r", close_task)
