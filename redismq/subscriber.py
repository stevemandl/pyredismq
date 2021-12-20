"""
Subscriber for RedisMQ
"""
from __future__ import annotations

import asyncio
import json
import time

from functools import partial
from typing import TYPE_CHECKING, Any, Callable, Dict, List, Optional, cast

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
TIMEOUT = 10.0


@debugging
class Subscriber:
    """
    Subscribes to channels to read messages
    """

    client: Client
    channels: List[str]
    message_queue: asyncio.Queue

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

        self.pubsub = None
        self.message_queue = asyncio.Queue()
        self._reader_task = asyncio.create_task(self.reader())
        self._reader_running = asyncio.Event()

    async def reader(self):
        """
        This function runs as a continuous task, like a thread, that gets
        messages from any of its subscribed channels and puts them in the
        message_queue to be retrieved by the read() function.
        """
        Subscriber.log_debug("reader")

        pubsub = self.client.connection_pool.pubsub()
        Subscriber.log_debug("    - pubsub: %r", pubsub)

        await pubsub.subscribe(*self.channels)
        Subscriber.log_debug("    - subscribed")

        # let the client know its running
        self._reader_running.set()

        while True:
            self._get_message = pubsub.get_message(
                ignore_subscribe_messages=True, timeout=10.0
            )
            Subscriber.log_debug("    - _get_message: %r", self._get_message)

            message = await self._get_message
            Subscriber.log_debug("    - message: %r", message)
            if message is not None:
                await self.message_queue.put(message)
                Subscriber.log_debug("    - put in the queue")

        Subscriber.log_debug("    - reader exit")

    def read(self) -> PayloadFuture:
        """
        Read a message from any of the channels.
        """
        Subscriber.log_debug("read")

        # create a future to hold the result
        read_result: PayloadFuture = asyncio.Future()

        # if the queue has something in it, get it
        if not self.message_queue.empty():
            message = self.message_queue.get_nowait()
            Subscriber.log_debug("    - message: %r", message)

            payload = Payload(self, message["channel"], message["data"])
            Subscriber.log_debug("    - payload: %r", payload)

            read_result.set_result(payload)

        else:
            # create a task to get the next message in the queue
            get_message_queue = asyncio.create_task(self.message_queue.get())
            Subscriber.log_debug("    - get_message_queue: %r", get_message_queue)

            get_message_queue.add_done_callback(
                partial(self._get_message_queue_callback, read_result)
            )

            # add a callback to the read_result which will be called when the task
            # has received a message and wrapped it in a Payload, or if the
            # read_result gets canceled
            read_result.add_done_callback(
                partial(self._read_result_callback, get_message_queue)
            )

        Subscriber.log_debug("    - read_result: %r", read_result)
        return read_result

    def _get_message_queue_callback(self, read_result, get_message_queue) -> None:
        """
        This callback function is called when a message has been retrieved by
        the read() function or the get_message_queue task has been canceled.
        Rather than simply copying the content from the queue to the
        read_result future, this wraps the content in a Payload.
        """
        Subscriber.log_debug(
            "_get_message_queue_callback %r %r", read_result, get_message_queue
        )

        # if the task for getting the message from the queue is cancelled don't
        # attempt to get the result
        if get_message_queue.cancelled():
            Subscriber.log_debug("    - cancelled")
            return

        message = get_message_queue.result()
        Subscriber.log_debug("    - message: %r", message)

        payload = Payload(self, message["channel"], message["data"])
        Subscriber.log_debug("    - payload: %r", payload)

        read_result.set_result(payload)

    def _read_result_callback(
        self, get_message_queue, read_result: PayloadFuture
    ) -> None:
        """
        This callback function is called when the read_result future has
        been completed or when that future has been cancelled.  If it is
        canceled, the get_message_queue task also needs to be cancelled.
        """
        Subscriber.log_debug(
            "_read_result_callback %r %r", get_message_queue, read_result
        )

        # if the read_result has been canceled, cancel the task for getting
        # the next message
        if read_result.cancelled():
            Subscriber.log_debug(f"   - read is canceled")
            if not get_message_queue.cancelled():
                Subscriber.log_debug(f"   - cancelling get_message_queue")
                get_message_queue.cancel()


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

    def __init__(self, subscriber: Subscriber, channel: str, message: str) -> None:
        Payload.log_debug("__init__ %r %r %r", subscriber, channel, message)

        self.subscriber = subscriber
        self.channel = channel
        try:
            self.message = json.loads(message)
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
