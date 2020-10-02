"""
test3.py
"""
import asyncio

from redismq import Client

async def read_a_confirmed_message():
    'test reading a confirmed message'
    mq_connection = await Client.connect('redis://mq.emcs.cucloud.net')
    my_consumer = await mq_connection.consumer('mystream', 'mygroup', 'consumer1')
    payload = await my_consumer.read()
    print('Consumer got message', payload.message)
    resp = 'I got your message' if payload.responseChannel else ''
    await payload.ack(resp)

if __name__ == '__main__':
    asyncio.run(read_a_confirmed_message())

