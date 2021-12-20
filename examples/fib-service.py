"""
Fibonacci Function

This sample application is a "confirming consumer" (a.k.a. RPC server) of
requests for the nth Fibonacci number.
"""
from __future__ import annotations

import os
import sys
import logging
import asyncio

from redismq.debugging import debugging, create_log_handler
from redismq import Client, Consumer, Producer
from redismq.consumer import Payload

from typing import cast

_log = logging.getLogger(__name__)
create_log_handler(_log, level=logging.DEBUG)


def getenv(key: str) -> str:
    """Get the value of an environment variable and throw an error if the value
    isn't set."""
    value = os.getenv(key)
    if value is None:
        raise RuntimeError("environment variable: %r" % (key,))
    return value


# settings
REDIS_HOST = getenv("REDIS_HOST")
REDIS_PORT = int(getenv("REDIS_PORT"))
REDIS_DB = int(getenv("REDIS_DB"))


# globals
mq: Client
consumer: Consumer
producer: Producer


@debugging
async def fib(payload: Payload) -> None:
    "test reading a confirmed message"
    global producer
    fib.log_debug("fib %r", payload)  # type: ignore[attr-defined]

    n: int
    result: int

    n = cast(int, payload.message)
    if n < 0:
        return await payload.ack(-1)

    fib.log_debug("    - n: %r", n)  # type: ignore[attr-defined]

    if n == 0:
        result = 0
    elif n == 1:
        result = 1
    else:
        try:
            part1 = asyncio.create_task(producer.addConfirmedMessage(n - 1))
            part2 = asyncio.create_task(producer.addConfirmedMessage(n - 2))
            await asyncio.gather(part1, part2)

            part1_result = part1.result()
            fib.log_debug("    - part1_result: %r", part1_result)  # type: ignore[attr-defined]
            part1_error = part1_result.get("error", None)
            if part1_error:
                raise RuntimeError("part1 error: " + part1_error)

            part2_result = part2.result()
            fib.log_debug("    - part2_result: %r", part2_result)  # type: ignore[attr-defined]
            part2_error = part2_result.get("error", None)
            if part2_error:
                raise RuntimeError("part2 error: " + part2_error)

            result = part1_result["message"] + part2_result["message"]

        except Exception as err:
            fib.log_debug("    - exception: %r", err)  # type: ignore[attr-defined]
            await payload.ack(error=str(err))
            part1.cancel()
            part2.cancel()
            return

    fib.log_debug("    - result: %r", result)  # type: ignore[attr-defined]
    await payload.ack(response=result)


@debugging
def _fib_task_callback(fib_task):
    _fib_task_callback.log_debug("_fib_task_callback %r", fib_task)  # type: ignore[attr-defined]


@debugging
async def main() -> None:
    """
    Main method
    """
    main.log_debug("starting...")  # type: ignore[attr-defined]
    global consumer, producer

    consumer_name = sys.argv[1]

    mq = await Client.connect(f"redis://{REDIS_HOST}:{REDIS_PORT}/{REDIS_DB}")
    consumer = await mq.consumer("fibStream", "fibGroup", consumer_name)
    producer = await mq.producer("fibStream")
    while True:
        payload = await consumer.read()
        main.log_debug("    - payload: %r", payload)  # type: ignore[attr-defined]

        fib_task = asyncio.create_task(fib(payload))
        fib_task.add_done_callback(_fib_task_callback)


if __name__ == "__main__":
    asyncio.run(main())
