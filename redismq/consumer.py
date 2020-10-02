"""
Consumer for RedisMQ
"""

class Consumer: # pylint: disable=too-few-public-methods
    """
    Consumes messages
    """
    def __init__(
        self,
        client,
        stream,
        groupName,
        consumerName,
        claim_stale_messages=True,
        min_idle_time=60000,
        scan_pending_on_start : bool=True) -> None:
        """
        default constructor
        """
        self.client = client
        self.stream = stream
        self.group_name = groupName
        self.consumer_name = consumerName
        self.claim_stale_messages = claim_stale_messages
        self.min_idle_time = min_idle_time
        self.latest_id = '0' if scan_pending_on_start else None
        # TODO: set up worker to check for stale messages and claim them

    async def read(self):
        """
        read a message from the queue
        """
        payloads = await self.client.redis.xread_group(
            self.group_name,
            self.consumer_name,
            [self.stream, ],
            timeout=0,
            count=1,
            latest_ids=[self.latest_id, ],
            no_ack=False)
        if len(payloads) == 0:
            if self.latest_id is not None:
                self.latest_id = None
                return self.read()
            # (else)
            raise Exception('xread_group got empty list even though requesting special id ">".')
        # (else)
        return self.Payload(self, payloads[0])

    class Payload:
        """
        Encapsulates the payload wrapped around a message and exposes an ack() function
        """
        def __init__(self, consumer, serialized_payload):
            self.message = serialized_payload.message
            self.consumer = consumer
            if 'response_channel' in serialized_payload:
                self.response_channel = serialized_payload.response_channel
            else:
                self.response_channel = None

        async def ack(self, response):
            """
            acks the message on the stream and pulishes the reponse on the responseChannel,
            if defined
            """
            await self.consumer.client.redis.xack(
                self.consumer.stream,
                self.consumer.group_name,
                self.message.id)
            if self.response_channel is not None:
                await self.consumer.client.redis.publish(self.response_channel, response)
