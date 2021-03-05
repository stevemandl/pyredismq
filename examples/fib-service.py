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
async def fib(payload: Consumer.Payload) -> None:
    "test reading a confirmed message"
    global producer

    try:
        n = payload.message
        if n < 0:
            return await payload.ack(-1)

        fib.log_debug("    - n: %r", n)  # type: ignore[attr-defined]

        if n == 0:
            result = 0
        elif n == 1:
            result = 1
        else:
            part1 = asyncio.create_task(producer.addConfirmedMessage(n - 1))
            part2 = asyncio.create_task(producer.addConfirmedMessage(n - 2))
            await asyncio.gather(part1, part2)

            fib.log_debug("    - part1: %r", part1)  # type: ignore[attr-defined]
            fib.log_debug("    - part2: %r", part2)  # type: ignore[attr-defined]

            result = part1.result()["message"] + part2.result()["message"]

        fib.log_debug("    - result: %r", result)  # type: ignore[attr-defined]
    except Exception as err:
        fib.log_debug("    - exception: %r", err)  # type: ignore[attr-defined]
        response = -1

    await payload.ack(response=result)


@debugging
async def main() -> None:
    """
    Main method
    """
    main.log_debug("starting...")  # type: ignore[attr-defined]
    global consumer, producer

    consumer_name = sys.argv[1]

    mq = await Client.connect("redis://mq.emcs.cucloud.net")
    consumer = await mq.consumer("fibStream", "fibGroup", consumer_name)
    producer = await mq.producer("fibStream")
    while True:
        payload = await consumer.read()
        main.log_debug("    - payload: %r", payload)  # type: ignore[attr-defined]
        asyncio.create_task(fib(payload))


if __name__ == "__main__":
    asyncio.run(main())
