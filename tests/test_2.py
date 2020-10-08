"""
test2.py
"""
import pytest
from redismq import Client

@pytest.mark.asyncio
async def test_send_an_unconfirmed_message():
    'test unconfirmed message'
    mq_connection = await Client.connect('redis://mq.emcs.cucloud.net')
    my_producer = await mq_connection.producer('mystream')
    response = await my_producer.addUnconfirmedMessage('Hello there!')
    print('Got message ID', response)
