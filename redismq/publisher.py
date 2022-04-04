"""
Publisher for RedisMQ
"""
from __future__ import annotations

import asyncio
import json

from typing import TYPE_CHECKING, Any, Callable, List

from .debugging import debugging

if TYPE_CHECKING:
    # circular reference
    from .client import Client

# settings
MAXLEN = 100


@debugging
class Publisher:
    """
    Publishes messages
    """

    client: Client
    channels: List[str]

    log_debug: Callable[..., None]

    def __init__(
        self,
        client: Client,
        channels: List[str] = [],
        maxlen: int = MAXLEN,
    ) -> None:
        """
        default constructor
        """
        Publisher.log_debug("__init__ %r %r", client, channels)

        self.client = client
        self.channels = channels
        self.maxlen = maxlen

    # pylint: disable=invalid-name
    async def publish(self, message: Any, channels: List[str] = []) -> None:
        """
        Publish a message to all of the channels.
        """
        Publisher.log_debug("publish %r", message)

        # JSON encode the message
        payload = {"message": json.dumps(message)}

        # publish to the ctor channels plus any extras
        pub_channels = set(self.channels).union(channels)

        # batch these up in a pipline
        pipe = self.client.redis.pipeline()
        futures = []
        for channel in pub_channels:
            futures.append(pipe.xadd(channel, payload, maxlen=self.maxlen))

        # not sure why these are in this order
        result = await pipe.execute()
        await asyncio.gather(*futures)
        Publisher.log_debug("    - result: %r", result)

        return result
