"""
IO Server

This sample application is a "confirming consumer" of requests for 'ioRead'
requests for an object.  The object name is the parameter, but in general the
parameters would be specific to the protocol (such as IPv4 address, unit number,
register number, number of registers, object identifier, property identifier,
array index, etc.)

This application takes the protocol channel name (see the IO director for
names), the group name (probably 'servers') and consumer name (like 'server-a',
'server-b', etc.)
"""
from __future__ import annotations

import os
import sys
import asyncio
import inspect
import random

from typing import Any, Callable, Dict

from redismq.debugging import debugging
from redismq import Client, Consumer


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
consumer: Consumer

function_map: Dict[str, Callable[..., Any]] = {}


def register_function(fn):
    function_map[fn.__name__] = fn


@register_function
async def ioRead(*, objName: str) -> float:
    """
    Return a random number.
    """
    dispatch.log_debug("ioRead %r", objName)  # type: ignore[attr-defined]
    return random.random() * 100.0


@debugging
async def dispatch(payload: Consumer.Payload) -> None:
    """
    Interpret the JSON encoded payload request and call the associated
    function.
    """
    dispatch.log_debug("dispatch %r", payload)  # type: ignore[attr-defined]
    global mq, function_map

    try:
        request = payload.message
        dispatch.log_debug("    - request: %r", request)  # type: ignore[attr-defined]
        if not isinstance(request, dict):
            raise TypeError("JSON object expected")

        # look for the function to call
        fn = request.pop("fn", None)
        if fn is None:
            raise RuntimeError("missing 'fn'")
        if fn not in function_map:
            raise NameError(f"function {fn!r} is not defined")

        # call the function and await the response if necessary
        result = function_map[fn](**request)
        if inspect.isawaitable(result):
            result = await result
        dispatch.log_debug("    - result: %r", result)  # type: ignore[attr-defined]

        # success
        await payload.ack(response=result)

    except Exception as err:
        return await payload.ack(error=str(err))


@debugging
async def main() -> None:
    """
    Main method
    """
    main.log_debug("starting...")  # type: ignore[attr-defined]
    global consumer

    stream_name = sys.argv[1]
    group_name = sys.argv[2]
    consumer_name = sys.argv[3]

    mq = await Client.connect(
        f"redis://{EMCS_REDIS_HOST}:{EMCS_REDIS_PORT}/{EMCS_REDIS_DB}"
    )
    consumer = await mq.consumer(stream_name, group_name, consumer_name)
    while True:
        payload = await consumer.read()
        main.log_debug("    - payload: %r", payload)  # type: ignore[attr-defined]
        asyncio.create_task(dispatch(payload))


if __name__ == "__main__":
    asyncio.run(main())
