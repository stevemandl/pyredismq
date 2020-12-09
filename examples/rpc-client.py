"""
Confirming Producer
"""
import asyncio
import json

from typing import Any

from redismq.debugging import debugging
from redismq import Client, Producer

mq: Client
producer: Producer


async def dispatch(fn_name: str, *args: Any, **kwargs: Any) -> Any:
    """
    Generic RPC dispatch.
    """
    global producer

    # send the request, wait for the response
    response = await producer.addConfirmedMessage({"fn": fn_name, **kwargs})

    # interpret as a function result or an exception
    if "result" in response:
        return response["result"]
    elif "error" in response:
        raise Exception(response["error"])
    else:
        raise RuntimeError("response must include 'result' or 'error'")


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
    mq = await Client.connect("redis://localhost")
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
