"""
Confirming Consumer
"""
import os
import asyncio
import time

from redismq.debugging import debugging
from redismq import Client, Consumer

count = 0
start = time.perf_counter()


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


@debugging
async def read_and_confirm(consumer: Consumer) -> None:
    "test reading a confirmed message"
    global count
    count = count + 1
    if count % 1000 == 0:
        elapsed = time.perf_counter() - start
        print(
            f"read_and_confirm {count:d} messages in {elapsed:0.4f} seconds, {count/elapsed:0.4f} msgs/sec"
        )
    payload = await consumer.read()
    read_and_confirm.log_debug("read payload %s", payload)  # type: ignore[attr-defined]

    resp = "I got your message" if payload.response_channel else "no response"
    await payload.ack(resp)


@debugging
async def main() -> None:
    """
    Main method
    """
    main.log_debug("starting...")  # type: ignore[attr-defined]
    mq = await Client.connect(
        f"redis://{REDIS_HOST}:{REDIS_PORT}/{REDIS_DB}"
    )
    consumer = await mq.consumer("testStream", "testGroup", "pyconsumer1")
    while True:
        await read_and_confirm(consumer)


if __name__ == "__main__":
    asyncio.run(main())
