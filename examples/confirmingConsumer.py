"""
confirmingConsumer.py
"""
import asyncio
import time 

from redismq.Debugging import FunctionLogging, Logging
from redismq import Client

count = 0
start = time.perf_counter()

@FunctionLogging
async def read_and_confirm(my_consumer):
    'test reading a confirmed message'
    global count
    count = count + 1
    if count % 1000 == 0:
        elapsed = time.perf_counter() - start
        print(f"read_and_confirm {count:d} messages in {elapsed:0.4f} seconds, {count/elapsed:0.4f} msgs/sec")
    payload = await my_consumer.read()
    read_and_confirm.log_debug("read payload %s", payload)
    resp = 'I got your message' if payload.response_channel else 'no response'
    await payload.ack(resp)

@FunctionLogging
async def main():
    """
    Main method
    """
    main.log_debug("starting...")
    connection = await Client.connect('redis://localhost')
    consumer = await connection.consumer('testStream', 'testGroup', 'pyconsumer1')
    while True:
        await read_and_confirm(consumer)

if __name__ == '__main__':
    asyncio.run(main())
