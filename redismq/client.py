"""
Client for RedisMQ
"""

import asyncio
import aioredis
from redismq.Debugging import FunctionLogging, Logging
from .producer import Producer
from .consumer import Consumer

__all__ = [ 'Client' ]

class Client(Logging):
    """
    Top level class to interface with message queue
    """
    def __init__(self):
        """
        default constructor - use connect() instead
        """
        Client._debug('init')
        self.redis = None
        self.sub_redis = None
        self._namespace = 'rmq'

    @classmethod
    async def connect(cls, address, redismq_namespace: str = None):
        """
        use this instead of the default constructor
        """
        self = Client()
        self.redis = await aioredis.create_redis(address, encoding='utf-8')
        self.sub_redis = await aioredis.create_redis(address, encoding='utf-8')
        if redismq_namespace:
            self._namespace = redismq_namespace # pylint: disable=protected-access
        return self

    @FunctionLogging
    async def producer(self, stream: str) -> Producer:
        """
        use this to get a Producer
        """
        print( asyncio.current_task())
        Client.producer.log_debug('producer %s', stream)
        return Producer(self, stream)

    @FunctionLogging
    async def consumer(
        self,
        stream,
        group_name,
        consumer_id,
        claim_stale_messages=True,
        min_idle_time=60000,
        scan_pending_on_start=True):
        """
        use this to get a Consumer
        """
        try:
            info = await self.redis.xinfo_groups(stream)
            Client.consumer.log_debug('xinfo %s', info)
        except aioredis.RedisError  as err:
            Client.consumer.log_debug(err)
            info = []
        if not stream in info:
            await self.redis.xgroup_create(
                stream,
                group_name,
                latest_id='$',
                mkstream=True)
            Client.consumer.log_debug('added group %s to stream %s ', group_name, stream)
        return Consumer(
            self,
            stream,
            group_name,
            consumer_id,
            claim_stale_messages,
            min_idle_time,
            scan_pending_on_start)
