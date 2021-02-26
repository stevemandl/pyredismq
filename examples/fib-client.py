"""
Fibonacci Function

This sample application is a "confirming consumer" of requests for the nth
Fibonacci number.
"""
import asyncio

from redismq.debugging import debugging
from redismq import Client


@debugging
async def main() -> None:
    """
    Main method
    """
    main.log_debug("starting...")  # type: ignore[attr-defined]
    mq = await Client.connect("redis://mq.emcs.cucloud.net")
    producer = await mq.producer("fibStream")
    while True:
        try:
            line = input("? ")
        except KeyboardInterrupt:
            break
        except EOFError:
            break
        result = await producer.addConfirmedMessage(int(line))
        print(f"result: {result!r}")
        print("")


if __name__ == "__main__":
    asyncio.run(main())
