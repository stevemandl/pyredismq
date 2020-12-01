"""
Consumer for RedisMQ
"""
from __future__ import annotations

from typing import TYPE_CHECKING, Any, Dict, Callable

from .debugging import debugging

if TYPE_CHECKING:
    from .client import Client


@debugging
class Consumer:  # pylint: disable=too-few-public-methods
    """
    Consumes messages
    """

    client: Client
    stream_name: str
    group_name: str
    consumer_name: str
    min_idle_time: int

    log_debug: Callable[..., None]

    def __init__(
        self,
        client: Client,
        stream_name: str,
        group_name: str,
        consumer_name: str,
        min_idle_time: int = 60000,
        # claim_stale_messages: bool = True,
        # scan_pending_on_start: bool = True,
    ) -> None:
        """
        default constructor
        """
        self.client = client
        self.stream_name = stream_name
        self.group_name = group_name
        self.consumer_name = consumer_name
        self.min_idle_time = min_idle_time

        # TODO: set up worker to check for stale messages and claim them
        # self.claim_stale_messages = claim_stale_messages
        # self.latest_id = "0" if scan_pending_on_start else ">"

        self.latest_id = ">"

    async def read(self) -> "Payload":
        """
        Read a message from the stream.
        """
        args = {
            "group_name": self.group_name,
            "consumer_name": self.consumer_name,
            "streams": [self.stream_name],
            "timeout": 0,
            "count": 1,
            "latest_ids": [self.latest_id],
            "no_ack": False,
        }
        Consumer.log_debug("read %r", args)

        with (await self.client.redis) as connection:
            while True:
                messages = await connection.xread_group(**args)
                Consumer.log_debug("    - messages: %r", messages)
                if messages:
                    break

        (stream, msg_id, payload) = messages[0]
        payload_dict = dict(payload)
        Consumer.log_debug(
            "    - stream %s, id %s, payload %s", stream, msg_id, payload_dict
        )
        return self.Payload(self, msg_id, payload_dict)

    class Payload:
        """
        Encapsulates the payload wrapped around a message and exposes an ack()
        function.
        """

        def __init__(
            self, consumer: "Consumer", msg_id: str, payload_dict: Dict[str, Any]
        ) -> None:
            self.message = payload_dict["message"]
            self.consumer = consumer
            self.msg_id = msg_id
            self.response_channel = payload_dict.get("response_channel", None)

        async def ack(self, response: str) -> None:
            """
            Acks the message on the stream and publishes the response on the
            responseChannel, if provided.
            """
            Consumer.log_debug("ack %r", response)

            Consumer.log_debug("    - msg_id: %r", self.msg_id)
            with (await self.consumer.client.redis) as connection:
                await connection.xack(
                    self.consumer.stream_name, self.consumer.group_name, self.msg_id
                )
                if self.response_channel is not None:
                    Consumer.log_debug(
                        "    - response channel: %r", self.response_channel
                    )

                    payload = {"message": response}
                    Consumer.log_debug("    - payload: %r", payload)

                    await connection.publish_json(self.response_channel, payload)

        async def nack(self, error: str) -> None:
            """
            Acks the message on the stream and publishes the error response
            on the responseChannel, if provided.
            """
            Consumer.log_debug("nack %r", error)

            Consumer.log_debug("    - msg_id: %r", self.msg_id)
            with (await self.consumer.client.redis) as connection:
                await connection.xack(
                    self.consumer.stream_name, self.consumer.group_name, self.msg_id
                )
                if self.response_channel is not None:
                    Consumer.log_debug(
                        "    - response channel: %r", self.response_channel
                    )

                    payload = {"error": error}
                    Consumer.log_debug("    - payload: %r", payload)

                    await connection.publish_json(self.response_channel, payload)
