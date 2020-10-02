"""
test1.py
"""
import asyncio

from redismq import Client

async def send_a_confirmed_message():
    'test confirmed message'
    mq_connection = await Client.connect('redis://mq.emcs.cucloud.net')
    my_producer = mq_connection.producer('mystream')
    response = await my_producer.addConfirmedMessage('Hello there! Let me know when you get this.')
    print('Got confirmation', response)

if __name__ == '__main__':
    asyncio.run(send_a_confirmed_message())
