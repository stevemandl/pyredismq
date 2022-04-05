"""
Subscriber for RedisMQ
"""
from __future__ import annotations

import sys
import time
import asyncio
import json

from functools import partial
from typing import TYPE_CHECKING, Any, Callable, List

from .debugging import debugging

if TYPE_CHECKING:
    # circular reference
    from .client import Client

    # class is declared as generic in stubs but not at runtime
    PayloadFuture = asyncio.Future[Any]
else:
    PayloadFuture = asyncio.Future

# settings
MAXLEN = 100
TIMEOUT = 10


@debugging
class Subscriber:
    """
    Subscribes to channels to read messages
    """

    client: Client
    channels: List[str]

    log_debug: Callable[..., None]

    def __init__(
        self,
        client: Client,
        channels: List[str],
    ) -> None:
        """
        default constructor
        """
        Subscriber.log_debug("__init__ %r %r", client, channels)

        self.client = client
        self.channels = channels
        self.latest_ids = {}
        self.xread_timeout = TIMEOUT * 1000  # milliseconds

    def read(self) -> PayloadFuture:
        """
        Read a message from the stream.
        """
        Subscriber.log_debug("read")

        # create a future to hold the result
        read_result: PayloadFuture = asyncio.Future()

        # create a task to read the next message
        get_message_task = asyncio.create_task(self.get_message(read_result))
        Subscriber.log_debug("    - get_message_task: %r", get_message_task)

        get_message_task.add_done_callback(self._get_message_task_callback)

        # add a callback to the read_result which will be called when the task
        # has received a message and wrapped it in a Payload, or if the
        # read_result gets canceled
        read_result.add_done_callback(partial(self._read_done, get_message_task))

        return read_result

    def _get_message_task_callback(self, *args):
        """
        Callback function for getting a message from the stream, used for
        debugging.
        """
        Subscriber.log_debug("_get_message_task_callback %r", args)

    def _read_done(
        self, get_message_task: asyncio.Task, read_result: PayloadFuture
    ) -> None:
        """
        Callback for read()
        """
        Subscriber.log_debug("_read_done %r %r", get_message_task, read_result)

        # if the read_result has been canceled, cancel the task for getting
        # the next message
        if read_result.cancelled():
            Subscriber.log_debug(f"   - read(%s) is canceled", self.consumer_name)
            get_message_task.cancel()
            Subscriber.log_debug(f"   - %r is canceled", get_message_task)

    async def get_message(self, read_result: PayloadFuture) -> None:
        """
        Get the next message in the stream.
        """
        Subscriber.log_debug("get_message %r", read_result)

        while True:
            Subscriber.log_debug("    - latest_ids: %r", self.latest_ids)

            try:
                pong = await self.client.redis.ping()
                Subscriber.log_debug("    - pong: %r", pong)

                set_result = await self.client.redis.set("x", time.time())
                Subscriber.log_debug("    - set_result: %r", set_result)

                # mock
                # messages = ((self.channels[0], (("0-0", (("message", '"hi there"'),)),)),)
                messages = await self.client.redis.xread(
                    streams=self.latest_ids, count=1, block=self.xread_timeout
                )
            except Exception as err:
                Subscriber.log_debug("    - xread exception: %r", err)
                raise err

            if messages:
                break

            Subscriber.log_debug("    - timeout")

        stream, element_list = messages[0]
        Subscriber.log_debug("    - stream, element_list: %r %r", stream, element_list)

        msg_id, payload = element_list[0]
        Subscriber.log_debug("    - msg_id, payload: %r %r", msg_id, payload)
        sys.stderr.write(f"payload: {payload!r}\n")

        payload_dict = dict(payload)
        Subscriber.log_debug(
            "    - stream %s, id %s, payload_dict %s", stream, msg_id, payload_dict
        )
        sys.stderr.write(f"payload_dict: {payload_dict!r}\n")

        # save the message ID so the next time this is entered it gets the
        # next message
        self.latest_ids[stream] = msg_id

        # build a Payload wrapper around the message
        payload = Payload(self, stream, payload_dict)
        Subscriber.log_debug("    - payload: %r", payload)

        # return this payload back to the application
        read_result.set_result(payload)


@debugging
class Payload:
    """
    Encapsulates the payload wrapped around a message and exposes an ack()
    function.
    """

    subscriber: Subscriber
    channel: str
    message: Any

    log_debug: Callable[..., None]

    def __init__(
        self, subscriber: Subscriber, channel: str, payload_dict: Dict[str, Any]
    ) -> None:
        Payload.log_debug("__init__ %r %r %r", subscriber, channel, payload_dict)

        self.subscriber = subscriber
        self.channel = channel
        try:
            self.message = json.loads(payload_dict["message"])
        except json.decoder.JSONDecodeError:
            Payload.log_debug("    - unable to decode message, log this event")
            return

        # this payload is now considered 'active', something is going to work
        # on it until it is acknowledged
        self.subscriber.client.active(self)

    async def ack(self) -> None:
        """
        Acks the payload, this message has been processed.
        """
        Payload.log_debug("ack")

        # this payload is now considered 'inactive'
        self.subscriber.client.inactive(self)

    def __repr__(self) -> str:
        """
        A relatively compact way of representing the payload when the message
        is substantial.
        """
        msg = repr(self.message)
        if len(msg) > 10:
            msg = msg[:5] + "..." + msg[len(msg) - 5 :]
        return f"<Payload channel={self.channel} message={msg}>"
