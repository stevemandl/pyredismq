"""
Confirming Producer
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
    mq = await Client.connect("redis://localhost")
    producer = await mq.producer("testStream")
    while True:
        try:
            line = input("? ")
        except KeyboardInterrupt:
            break
        except EOFError:
            break
        response = await producer.addConfirmedMessage(line)
        print(response)
        print("")


if __name__ == "__main__":
    asyncio.run(main())
