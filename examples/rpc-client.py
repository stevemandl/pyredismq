"""
Remote Procedure Call Client

This application uses a producer to generate a "confirmed" request to the
stream for the server and waits for the response.
"""
import os
import asyncio

from typing import Any

from redismq.debugging import debugging
from redismq import Client, Producer


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


mq: Client
producer: Producer


async def dispatch(fn_name: str, *args: Any, **kwargs: Any) -> Any:
    """
    Generic RPC dispatch.
    """
    global producer

    # send the request, wait for the response
    response = await producer.addConfirmedMessage({"fn": fn_name, **kwargs})
    # print(f"response: {response!r}")

    # interpret as a function result or an exception
    error = response.get("error", None)
    if error:
        raise Exception(error)

    # the "message" in the response is the value of the "response"
    # parameter when ack() function was called
    return response["message"]


#
#
#


async def upper(*, text: str = "") -> str:
    """
    Translate the text to all uppercase.
    """
    return await dispatch(upper.__name__, text=text)


@debugging
async def main() -> None:
    """
    Main method
    """
    global mq, producer

    main.log_debug("starting...")  # type: ignore[attr-defined]
    mq = await Client.connect(
        f"redis://{EMCS_REDIS_HOST}:{EMCS_REDIS_PORT}/{EMCS_REDIS_DB}"
    )
    producer = await mq.producer("testStream")
    while True:
        try:
            line = input("? ")
        except KeyboardInterrupt:
            break
        except EOFError:
            break

        try:
            result = await upper(text=line)
            print(f"result: {result!r}")
        except Exception as err:
            print(f"exception: {err!r}")
        print("")


if __name__ == "__main__":
    asyncio.run(main())
