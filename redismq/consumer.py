"""
Consumer for RedisMQ
"""
from redismq.Debugging import FunctionLogging, Logging

class Consumer(Logging): # pylint: disable=too-few-public-methods
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
        self.latest_ids = ['0',] if scan_pending_on_start else ['>',]
        # TODO: set up worker to check for stale messages and claim them

    @FunctionLogging
    async def read(self):
        """
        read a message from the queue
        """
        args = {
            'group_name': self.group_name, 
            'consumer_name': self.consumer_name,
            'streams': [self.stream, ],
            'timeout': 0,
            'count': 1,
            'latest_ids': self.latest_ids,
            'no_ack': False}
        Consumer.read.log_debug('reading args=%s', args)
        messages = await self.client.redis.xread_group(**args)
        Consumer.read.log_debug('read messages %s', messages)
        if len(messages) == 0:
            if self.latest_ids is not ['>',]:
                self.latest_ids = ['>',]
                return await self.read()
            # (else)
            raise Exception('xread_group got empty list even though requesting special id ">".')
        # (else)
        (stream, msg_id, payload) = messages[0]
        payload_dict = dict(payload)
        Consumer.read.log_debug('stream %s, id %s, payload %s', stream, msg_id, payload_dict)
        return self.Payload(self, msg_id, payload_dict)

    class Payload:
        """
        Encapsulates the payload wrapped around a message and exposes an ack() function
        """
        def __init__(self, consumer, msg_id, serialized_payload):
            self.message = serialized_payload['message']
            self.consumer = consumer
            self.msg_id = msg_id
            if 'response_channel' in serialized_payload:
                self.response_channel = serialized_payload['response_channel']
            else:
                self.response_channel = None

        @FunctionLogging
        async def ack(self, response):
            """
            acks the message on the stream and pulishes the reponse on the responseChannel,
            if defined
            """
            Consumer.Payload.ack.log_debug(response)
            await self.consumer.client.redis.xack(
                self.consumer.stream,
                self.consumer.group_name,
                self.msg_id)
            if self.response_channel is not None:
                message = {"message": response}
                await self.consumer.client.redis.publish_json(self.response_channel, message)
