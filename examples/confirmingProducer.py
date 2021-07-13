"""
Confirming Producer
"""
import os
import asyncio

from redismq.debugging import debugging
from redismq import Client


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
async def main() -> None:
    """
    Main method
    """
    main.log_debug("starting...")  # type: ignore[attr-defined]
    mq = await Client.connect(
        f"redis://{REDIS_HOST}:{REDIS_PORT}/{REDIS_DB}"
    )
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
