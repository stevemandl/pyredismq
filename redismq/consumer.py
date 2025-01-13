"""
Consumer for RedisMQ
"""
from __future__ import annotations

import asyncio
import json
from functools import partial

from typing import TYPE_CHECKING, Any, Dict, TypedDict, Callable, Optional
from redis import Connection  # type: ignore[attr-defined]

from .debugging import debugging


class Client(TypedDict):
    redis: Connection


@debugging
class Consumer:  # pylint: disable=too-few-public-methods
    """
    Consumes messages from a stream
    """

    client: Client
    stream_name: str
    group_name: str
    consumer_name: str
    latest_id: bytes
    check_backlog: bool
    min_idle_time: int

    log_debug: Callable[..., None]

    def __init__(
        self,
        client: Client,
        stream_name: str,
        group_name: str,
        consumer_name: str,
        # scan_pending_on_start: bool = True,
        # claim_stale_messages: bool = True,
        min_idle_time: int = 60000,
    ) -> None:
        """
        default constructor
        """
        Consumer.log_debug(
            "__init__ %r %r %r %r", client, stream_name, group_name, consumer_name
        )

        self.client = client
        self.stream_name = stream_name
        self.group_name = group_name
        self.consumer_name = consumer_name
        # self.scan_pending_on_start = scan_pending_on_start
        # self.claim_stale_messages = claim_stale_messages
        self.min_idle_time = min_idle_time

        # by default just read new messages that haven't been delivered
        self.latest_id = b">"
        self.check_backlog = False
        self.xread_timeout = 10000  # milliseconds

    def read(self) -> PayloadFuture:
        """
        Read a message from the stream.
        """
        Consumer.log_debug("read(%s)", self.consumer_name)

        # create a future to hold the result
        read_result: PayloadFuture = asyncio.Future()

        # create a task to read the next message
        get_message_task = asyncio.create_task(self.get_message(read_result))
        Consumer.log_debug("    - get_message_task: %r", get_message_task)

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
        Consumer.log_debug("_get_message_task_callback %r", args)

    def _read_done(
        self, get_message_task: asyncio.Task, read_result: PayloadFuture
    ) -> None:
        """
        Callback for read()
        """
        Consumer.log_debug(
            "_read_done(%s) %r %r", self.consumer_name, get_message_task, read_result
        )

        # if the read_result has been canceled, cancel the task for getting
        # the next message
        if read_result.cancelled():
            Consumer.log_debug(f"   - read(%s) is canceled", self.consumer_name)
            get_message_task.cancel()
            Consumer.log_debug(f"   - %r is canceled", get_message_task)

    async def get_message(self, read_result: PayloadFuture) -> None:
        """
        Get the next message in the stream, checking the backlog first to see
        if there are any previously delivered messages that haven't been acked
        (like the consumer crashed processing the message).
        """
        Consumer.log_debug("get_message(%s) %r", self.consumer_name, read_result)

        while True:
            # if we are checking the backlog, get the next message otherwise
            # get the next one that hasn't been delivered to antoher consumer
            if self.check_backlog:
                latest_id = self.latest_id
            else:
                latest_id = b">"
            Consumer.log_debug("    - latest_id: %r", latest_id)

            args = {
                "groupname": self.group_name,
                "consumername": self.consumer_name,
                "count": 1,
                "block": self.xread_timeout,
                "streams": {self.stream_name: latest_id},
            }
            messages = None
            try:
                messages = await self.client.redis.xreadgroup(**args)
                Consumer.log_debug("    - messages: %r", messages)
            except Exception as err:
                Consumer.log_debug("    - xreadgroup exception: %r", err)

            if not messages:
                Consumer.log_debug("    - timeout")
                continue

            # no more messages means we have completely consumed the backlog
            stream, element_list = messages[0]
            if not element_list:
                self.check_backlog = False
            else:
                break

        msg_id, payload = element_list[0]
        payload_dict = dict(payload)
        Consumer.log_debug(
            "    - stream %s, id %s, payload_dict %s", stream, msg_id, payload_dict
        )

        # save the message ID so the next time this is entered it gets the
        # next message in the backlog
        self.latest_id = msg_id

        # build a Payload wrapper around the message
        payload = Payload(self, msg_id, payload_dict)
        Consumer.log_debug("    - payload: %r", payload)

        # return this payload back to the application
        read_result.set_result(payload)


@debugging
class Payload:
    """
    Encapsulates the payload wrapped around a message and exposes an ack()
    function.
    """

    consumer: Consumer
    msg_id: str
    response_channel: Optional[str]
    message: Dict[str, Any]

    log_debug: Callable[..., None]

    def __init__(
        self, consumer: Consumer, msg_id: str, payload_dict: Dict[str, Any]
    ) -> None:
        Payload.log_debug("__init__ %r %r", msg_id, payload_dict)

        self.consumer = consumer
        self.msg_id = msg_id
        self.response_channel = payload_dict.get("response_channel", None)
        try:
            self.message = json.loads(payload_dict["message"])
        except json.decoder.JSONDecodeError:
            Payload.log_debug("    - unable to decode message, log this event")
            asyncio.ensure_future(
                self.consumer.client.redis.xack(
                    self.consumer.stream_name, self.consumer.group_name, self.msg_id
                )
            )
            return

    async def ack(self, response: Any = None, error: Any = None) -> None:
        """
        Acks the message on the stream and publishes the response on the
        responseChannel, if provided.
        """
        Payload.log_debug("ack response=%r error=%r", response, error)

        Payload.log_debug("    - msg_id: %r", self.msg_id)
        await self.consumer.client.redis.xack(
            self.consumer.stream_name, self.consumer.group_name, self.msg_id
        )
        Payload.log_debug("    - xack complete")

        if self.response_channel is not None:
            Payload.log_debug("    - response channel: %r", self.response_channel)
            m_response = {"message": response, "error": error}
            await self.consumer.client.redis.publish(
                self.response_channel, json.dumps(m_response)
            )
            Payload.log_debug("    - published json")


if TYPE_CHECKING:
    # class is declared as generic in stubs but not at runtime
    PayloadFuture = asyncio.Future[Payload]  # type: ignore
else:
    PayloadFuture = asyncio.Future
