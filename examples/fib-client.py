"""
Fibonacci Function

This sample application is a "confirming consumer" (a.k.a. RPC client) of
requests for the nth Fibonacci number.
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
EMCS_REDIS_HOST = getenv("EMCS_REDIS_HOST")
EMCS_REDIS_PORT = int(getenv("EMCS_REDIS_PORT"))
EMCS_REDIS_DB = int(getenv("EMCS_REDIS_DB"))


@debugging
async def main() -> None:
    """
    Main method
    """
    main.log_debug("starting...")  # type: ignore[attr-defined]
    mq = await Client.connect(
        f"redis://{EMCS_REDIS_HOST}:{EMCS_REDIS_PORT}/{EMCS_REDIS_DB}"
    )
    producer = await mq.producer("fibStream")
    while True:
        try:
            line = input("? ")
            if not line:
                break
            n = int(line, base=0)
        except ValueError:
            print("integer expected")
            continue
        except KeyboardInterrupt:
            break
        except EOFError:
            break
        response = await producer.addConfirmedMessage(n)
        print(f"response: {response!r}")

        # interpret as a function result or an exception
        error = response.get("error", None)
        if error:
            raise Exception(error)

        # the "message" in the response is the value of the "response"
        # parameter when ack() function was called
        result = response["message"]

        print(f"result: {result!r}")
        print("")


if __name__ == "__main__":
    asyncio.run(main())
