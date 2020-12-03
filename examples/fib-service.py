"""
Fibonacci Function

This sample application is a "confirming consumer" of requests for the nth
Fibonacci number.
"""
from __future__ import annotations

import sys
import asyncio

from redismq.debugging import debugging
from redismq import Client, Consumer, Producer

mq: Client
consumer: Consumer
producer: Producer


@debugging
async def fib(payload: Consumer.Payload) -> int:
    "test reading a confirmed message"
    global producer

    n = payload.message
    if n < 0:
        return await payload.ack(-1)

    fib.log_debug("    - n: %r", n)  # type: ignore[attr-defined]

    if n == 0:
        response = 0
    elif n == 1:
        response = 1
    else:
        part1 = asyncio.create_task(producer.addConfirmedMessage(n - 1))
        part2 = asyncio.create_task(producer.addConfirmedMessage(n - 2))
        await asyncio.gather(part1, part2)

        fib.log_debug("    - part1: %r", part1)  # type: ignore[attr-defined]
        fib.log_debug("    - part2: %r", part2)  # type: ignore[attr-defined]

        response = part1.result() + part2.result()

    fib.log_debug("    - response: %r", response)  # type: ignore[attr-defined]

    await payload.ack(response)


@debugging
async def main() -> None:
    """
    Main method
    """
    main.log_debug("starting...")  # type: ignore[attr-defined]
    global consumer, producer

    consumer_name = sys.argv[1]

    mq = await Client.connect("redis://localhost")
    consumer = await mq.consumer("testStream", "testGroup", consumer_name)
    producer = await mq.producer("testStream")
    while True:
        payload = await consumer.read()
        main.log_debug("    - payload: %r", payload)  # type: ignore[attr-defined]
        asyncio.create_task(fib(payload))


if __name__ == "__main__":
    asyncio.run(main())
