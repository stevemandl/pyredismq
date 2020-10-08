"""
Producer for RedisMQ
"""
import asyncio
from redismq.Debugging import FunctionLogging, Logging

class Producer(Logging):
    """
    Produces messages
    """
    def __init__(self, client, stream: str) -> None:
        """
        default constructor
        """
        self.client = client
        self.stream = stream
        # pylint: disable=protected-access
        self._channel_key = '%s:responseid' % client._namespace


    async def _unique_channel_id(self) -> str:
        """
        utility method to get a global unique channel ID in the reserved namespace
        """
        uid = await self.client.redis.incr(self._channel_key)
        # pylint: disable=protected-access
        return '%s:response.%d' % ( self.client._namespace, uid )

    @FunctionLogging
    async def _resp_task(self, channel):
        """
        utility method for subsciber
        """
        await channel.wait_message()
        try:
            payload = await channel.get_json()
        except ValueError:
            payload = {"message": "JSON Decoding Error"}
        await self.client.sub_redis.unsubscribe(channel)
        return payload

    # pylint: disable=invalid-name
    @FunctionLogging
    async def addUnconfirmedMessage(self, message: str) -> str:
        """
        Add an unconfirmed message to the message queue
        """
        Producer.addUnconfirmedMessage.log_debug(message)
        mapping = { 'message': message }
        return await self.client.redis.xadd(self.stream, mapping)

    # pylint: disable=invalid-name
    @FunctionLogging
    async def addConfirmedMessage(self, message: str):
        """
        Add a confirmed message to the message queue
        and await a response. The subscriber is set up to listen for the response before
        sending the message, in case the response comes back before the subscriber has
        a chance to listen.
        """
        Producer.addConfirmedMessage.log_debug(message)
        response_channel = await self._unique_channel_id()
        Producer.addConfirmedMessage.log_debug('response_channel %s', response_channel)
        mapping = { 'message': message, 'response_channel': response_channel }
        ( channel, ) = await self.client.sub_redis.subscribe(response_channel)
        Producer.addConfirmedMessage.log_debug('channel %s', channel)
        task = asyncio.create_task(self._resp_task(channel))
        result = await self.client.redis.xadd(self.stream, mapping)
        Producer.addConfirmedMessage.log_debug('added message to stream %s, got %s', self.stream, result)
        return await task
