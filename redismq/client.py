"""
Client for RedisMQ
"""

import aioredis
from .producer import Producer
from .consumer import Consumer

__all__ = [ 'Client' ]

class Client:
    """
    Top level class to interface with message queue
    """
    def __init__(self):
        """
        default constructor - use connect() instead
        """
        self.redis = None
        self.sub_redis = None
        self._namespace = 'rmq'

    @classmethod
    async def connect(cls, address, redismq_namespace: str = None):
        """
        use this instead of the default constructor
        """
        self = Client()
        self.redis = await aioredis.create_redis(address)
        self.sub_redis = await aioredis.create_redis(address)
        if redismq_namespace:
            self._namespace = redismq_namespace # pylint: disable=protected-access
        return self

    def producer(self, stream: str) -> Producer:
        """
        use this to get a Producer
        """
        return Producer(self, stream)

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
            info = await self.redis.xinfo_groups(stream, encoding='utf-8')
            print('xinfo', info)
        except Exception:
            await self.redis.xgroup_create(
                stream,
                group_name,
                latest_id='$',
                mkstream=True)
        return Consumer(
            self,
            stream,
            group_name,
            consumer_id,
            claim_stale_messages,
            min_idle_time,
            scan_pending_on_start)
