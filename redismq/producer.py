"""
Producer for RedisMQ
"""
from __future__ import annotations

import asyncio
import json
import time

from typing import TYPE_CHECKING, Any, Callable, Dict, cast

from .debugging import debugging

if TYPE_CHECKING:
    # circular reference
    from .client import Client

    # class is declared as generic in stubs but not at runtime
    AnyFuture = asyncio.Future[Any]
else:
    AnyFuture = asyncio.Future


@debugging
class Producer:
    """
    Produces messages
    """

    client: Client
    stream_name: str
    channel_key: str
    id: int

    log_debug: Callable[..., None]

    def __init__(self, client: Client, stream_name: str) -> None:
        """
        default constructor
        """
        Producer.log_debug("__init__ %r %r", client, stream_name)

        self.client = client
        self.stream_name = stream_name
        self.channel_key = "%s:responseid" % client.namespace
        self.id = time.time_ns()

    async def _resp_task(
        self, payload: Dict[str, Any], response_channel_id: str
    ) -> Any:
        """
        utility method for a confirmed request
        """
        if response_channel_id is None:
            # get a unique channel identifier
            uid = await self.client.redis.incr(self.channel_key)
            response_channel_id = "%s:response.%d" % (self.client.namespace, uid)
        Producer.log_debug("    - response_channel_id: %r", response_channel_id)

        response = {}
        with await self.client.redis as sub_redis:
            try:
                # subscribe to the channel
                (response_channel,) = await sub_redis.subscribe(response_channel_id)
                Producer.log_debug("    - response_channel: %r", response_channel)

                # pack it into the request payload
                payload["response_channel"] = response_channel_id

                # put the request into the stream
                message_id: str = await self.client.redis.xadd(self.stream_name, payload)
                Producer.log_debug("    - message_id: %r", message_id)

                # wait for the response to come back
                await response_channel.wait_message()
                try:
                    response = await response_channel.get_json()
                except ValueError as err:
                    response["message"] = "JSON Decoding Error"
                    response["err"] = err

            except asyncio.CancelledError as err:
                Producer.log_debug("    - canceled %s", response_channel_id)
                response["message"] = "Cancelled Error"
                response["err"] = err
            finally:
                Producer.log_debug("    - finally %s", response_channel_id)
                await sub_redis.unsubscribe(response_channel_id)

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
        future = self.client.redis.xadd(self.stream_name, payload)

        return cast(AnyFuture, future)

    # pylint: disable=invalid-name
    def addConfirmedMessage(
        self, message: Any, response_channel_id: str = None
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
    def destroy( self ) -> None:
        """ destroy- stops this producer from working. This is automatically called when
            client.dispose_producer() is called with this producer
        """
        self.client = None
        self.stream_name = None
        Producer.log_debug("producer id %r destroyed", self.id)
