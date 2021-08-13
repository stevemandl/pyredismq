"""
Consumer for RedisMQ
"""
from __future__ import annotations

import asyncio
import json

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
        scan_pending_on_start: bool = True,
        claim_stale_messages: bool = True,
        min_idle_time: int = 60000,
    ) -> None:
        """
        default constructor
        """
        self.client = client
        self.stream_name = stream_name
        self.group_name = group_name
        self.consumer_name = consumer_name
        self.scan_pending_on_start = scan_pending_on_start
        self.claim_stale_messages = claim_stale_messages
        self.min_idle_time = min_idle_time

        # by default just read new messages that haven't been delivered
        self.latest_id = b">"
        self.ready = True
        self.xread_timeout = 5000

    # close()
    def close(self) -> None:
        """
        used to close a consumer gracefully if it is waiting to read
        """
        self.ready = False

    # to be used to cancel a read
    async def read(self) -> "Payload":
        """
        Read a message from the stream.
        """
        Consumer.log_debug("read")
        closed_payload = self.Payload(self, 0, {'message': '{"error": "closed"}'})
        if not self.ready:
            return closed_payload

        if self.scan_pending_on_start:
            (
                pending_count,
                min_id,
                max_id,
                pending_consumers,
            ) = await self.client.redis.xpending(self.stream_name, self.group_name)
            if pending_count:
                Consumer.log_debug(
                    f"    - pending summary {self.stream_name!r}: {pending_count!r}, \
                        {min_id!r}..{max_id!r}"
                )
                for pending_consumer, pending_consumer_count in pending_consumers:
                    if pending_consumer == self.consumer_name:
                        Consumer.log_debug("    - this consumer has pending messages")
                    else:
                        Consumer.log_debug(
                            f"    - pending messages for {pending_consumer!r}"
                        )

                    pending_messages = await self.client.redis.xpending(
                        self.stream_name,
                        self.group_name,
                        b"-",
                        b"+",
                        pending_consumer_count,
                        pending_consumer,
                    )
                    for (
                        msg_id,
                        consumer_name, # pylint: disable=unused-variable
                        idle_time,
                        delivered,
                    ) in pending_messages:
                        Consumer.log_debug(
                            f"        {msg_id!r}, idle {idle_time/1000.0!r}s, \
                                delivered {delivered!r}"
                        )
                        if self.claim_stale_messages:
                            retcode = await self.client.redis.xclaim(
                                self.stream_name,
                                self.group_name,
                                self.consumer_name,
                                self.min_idle_time,
                                msg_id,
                            )
                            Consumer.log_debug(f"        claim: {retcode!r}")

                            self.latest_id = b"0-0"
            else:
                Consumer.log_debug("    - no pending messages")

            # assume we'll get caught up
            self.scan_pending_on_start = False
        with await self.client.redis as conn:
            # only loop if ready, recheck periodically
            while self.ready:
                Consumer.log_debug("    - latest_id: %r redis: %r", self.latest_id, self.client.redis)

                args = {
                    "group_name": self.group_name,
                    "consumer_name": self.consumer_name,
                    "streams": [self.stream_name],
                    "timeout": self.xread_timeout,
                    "count": 1,
                    "latest_ids": [self.latest_id],
                    "no_ack": False,
                }
                messages = await conn.xread_group(**args)
                Consumer.log_debug("    - messages: %r", messages)
                if messages:
                    break

                # loop around again, read the next new message
                self.latest_id = b">"

        if not self.ready:
            return closed_payload
        (stream, msg_id, payload) = messages[0]
        payload_dict = dict(payload)
        Consumer.log_debug(
            "    - stream %s, id %s, payload %s", stream, msg_id, payload_dict
        )

        # assume this message is going to be processed, the next time read() is
        # called it will pick up the next claimed pending message, otherwise
        # loop around and read the next new message
        self.latest_id = msg_id

        return self.Payload(self, msg_id, payload_dict)

    class Payload:
        """
        Encapsulates the payload wrapped around a message and exposes an ack()
        function.
        """

        def __init__(
            self, consumer: "Consumer", msg_id: str, payload_dict: Dict[str, Any]
        ) -> None:
            Consumer.log_debug("Payload __init__ %r %r", msg_id, payload_dict)

            self.consumer = consumer
            self.msg_id = msg_id
            self.response_channel = payload_dict.get("response_channel", None)
            try:
                self.message = json.loads(payload_dict["message"])
            except json.decoder.JSONDecodeError:
                Consumer.log_debug("    - unable to decode message, log this event")
                asyncio.ensure_future(
                    self.consumer.client.redis.xack(
                        self.consumer.stream_name, self.consumer.group_name, self.msg_id
                    )
                )

        async def ack(self, response: Any = None, error: Any = None) -> None:
            """
            Acks the message on the stream and publishes the response on the
            responseChannel, if provided.
            """
            Consumer.log_debug("Payload ack %r %r", response, error)

            Consumer.log_debug("Payload     - msg_id: %r", self.msg_id)
            await self.consumer.client.redis.xack(
                self.consumer.stream_name, self.consumer.group_name, self.msg_id
            )
            Consumer.log_debug("Payload     - xack complete")
            if self.response_channel is not None:
                Consumer.log_debug(
                    "Payload     - response channel: %r", self.response_channel
                )
                m_response = {"message": response, "error": error}
                await self.consumer.client.redis.publish_json(self.response_channel, m_response)
                Consumer.log_debug("Payload     - published json")
