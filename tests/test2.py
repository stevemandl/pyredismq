"""
test2.py
"""
import asyncio

from redismq import Client

async def send_an_unconfirmed_message():
    'test unconfirmed message'
    mq_connection = await Client.connect('redis://mq.emcs.cucloud.net')
    my_producer = mq_connection.producer('mystream')
    response = await my_producer.addUnconfirmedMessage('Hello there!')
    print('Got message ID', response)

if __name__ == '__main__':
    asyncio.run(send_an_unconfirmed_message())
