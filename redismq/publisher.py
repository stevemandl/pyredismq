"""
Publisher for RedisMQ
"""
from __future__ import annotations

import asyncio
import json
import time

from typing import TYPE_CHECKING, Any, Callable, Dict, List, Optional, cast

from .debugging import debugging

if TYPE_CHECKING:
    # circular reference
    from .client import Client


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
    ) -> None:
        """
        default constructor
        """
        Publisher.log_debug("__init__ %r %r", client, channels)

        self.client = client
        self.channels = channels

    # pylint: disable=invalid-name
    async def publish(self, message: Any, channels: List[str] = []) -> None:
        """
        Publish a message to all of the channels.
        """
        Publisher.log_debug("publish %r", message)

        # JSON encode the message
        payload = json.dumps(message)

        # publish on the ctor channels plus any extras
        pub_channels = self.channels + channels

        numsub = await self.client.connection_pool.pubsub_numsub(*pub_channels)
        Publisher.log_debug("    - numsub: %r", numsub)

        # send a copy to each stream
        for channel, subscriber_count in numsub:
            if subscriber_count:
                await self.client.connection_pool.publish(channel, payload)
                Publisher.log_debug("    - published: %r", channel)
