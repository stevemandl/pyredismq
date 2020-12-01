"""
Unconfirming Producer
"""
import asyncio
import time

from redismq.debugging import debugging
from redismq import Client


@debugging
async def main() -> None:
    """
    Main method
    """
    main.log_debug("starting...")  # type: ignore[attr-defined]
    mq = await Client.connect("redis://localhost")
    producer = await mq.producer("testStream")
    while True:
        try:
            line = input("? ")
        except EOFError:
            break
        await producer.addUnconfirmedMessage(line)


if __name__ == "__main__":
    asyncio.run(main())
