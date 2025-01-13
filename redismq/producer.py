"""
Producer for RedisMQ
"""
from __future__ import annotations

import asyncio
import json
import time

from typing import TYPE_CHECKING, Any, Callable, Dict, TypedDict, Optional, cast
from redis import Connection  # type: ignore[attr-defined]
from .debugging import debugging

# class is declared as generic in stubs but not at runtime
AnyFuture = asyncio.Future

# settings
MAXLEN = 100
TIMEOUT = 10.0


class Client(TypedDict):
    redis: Connection


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

    # make the handler for the channel
    def get_handler(self, channel_id, fut: AnyFuture):
        Producer.log_debug("get_handler channel_id %r fut %r" % (channel_id, fut))

        async def _handler(json_message=None):
            Producer.log_debug("_handler json_message: %r", json_message)
            response = None
            try:
                if json_message:
                    response = json.loads(json_message["data"])
            except ValueError as err:
                Producer.log_debug("    - value/decoding error %s", channel_id)
                response = {"message": "JSON Decoding Error", "err": err}
            except TypeError as err:
                Producer.log_debug("    - type error %s", channel_id)
                response = {"message": "Type Error", "err": err}
            finally:
                Producer.log_debug("    - finally %s", channel_id)
                await self.client.pubsub.unsubscribe(channel_id)
                Producer.log_debug("    - unsubscribed %s", channel_id)
                if not fut.cancelled():
                    Producer.log_debug("    -setting result %s", response)
                    fut.set_result(response)

        return _handler

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
        future = self.client.redis.xadd(self.stream_name, payload, maxlen=self.maxlen)

        return cast(AnyFuture, future)

    # pylint: disable=invalid-name
    async def addConfirmedMessage(self, message: Any):
        """
        Adds a confirmed message to the message queue and
        results in the confirmed response.
        """
        Producer.log_debug("addConfirmedMessage %r", message)

        # JSON encode the message
        payload = {"message": json.dumps(message)}
        # create a future
        future = asyncio.get_running_loop().create_future()

        # get a unique channel identifier
        uid = await self.client.redis.incr(self.channel_key)
        response_channel_id = "%s:response.%d" % (self.client.namespace, uid)
        Producer.log_debug("    - response_channel_id: %r", response_channel_id)

        # pack it into the request payload
        payload["response_channel"] = response_channel_id

        # start listening for a response
        _handler = self.get_handler(response_channel_id, future)
        kwargs = {response_channel_id: _handler}
        try:
            await self.client.pubsub.subscribe(**kwargs)
            Producer.log_debug("    - subscribed")

            # put the request into the stream
            message_id: str = await self.client.redis.xadd(
                self.stream_name, payload, maxlen=self.maxlen
            )
            Producer.log_debug("    - message_id: %r", message_id)
            # future will get the result set by the handler when the response is published
            resp = await asyncio.wait_for(future, self.timeout)
        except asyncio.TimeoutError as err:
            Producer.log_debug("    - timeout waiting for future: %r", err)
            await _handler()
            resp = {"message": "Timeout Error", "err": err}
        except asyncio.CancelledError as err:
            Producer.log_debug("    - cancelled %r", err)
            await _handler()
            resp = {"message": "Cancelled Error", "err": err}
        except BaseException as err:
            # Producer.log_debug("    - unexpected error %r", err)
            await _handler()
            resp = {"message": "Unexpected Error", "err": err}
        return resp

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
