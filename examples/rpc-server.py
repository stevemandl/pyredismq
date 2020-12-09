"""
Fibonacci Function

This sample application is a "confirming consumer" of requests for the nth
Fibonacci number.
"""
from __future__ import annotations

import sys
import asyncio
import inspect
import json

from redismq.debugging import debugging
from redismq import Client, Consumer, Producer

mq: Client
consumer: Consumer

function_map: Dict[str,] = {}


def register_function(fn):
    function_map[fn.__name__] = fn


@register_function
async def upper(*, text: str) -> str:
    """
    Translate the text to all uppercase.
    """
    return text.upper()


@debugging
async def dispatch(payload: Consumer.Payload) -> None:
    """
    Interpret the JSON encoded payload request and call the associated
    function.
    """
    dispatch.log_debug("dispatch %r", payload)  # type: ignore[attr-defined]
    global function_map

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
        await payload.ack({"result": result})

    except Exception as err:
        return await payload.ack({"error": str(err)})


@debugging
async def main() -> None:
    """
    Main method
    """
    main.log_debug("starting...")  # type: ignore[attr-defined]
    global consumer, producer

    consumer_name = sys.argv[1]

    mq = await Client.connect("redis://localhost")
    consumer = await mq.consumer("testStream", "testGroup", consumer_name)
    while True:
        payload = await consumer.read()
        main.log_debug("    - payload: %r", payload)  # type: ignore[attr-defined]
        asyncio.create_task(dispatch(payload))


if __name__ == "__main__":
    asyncio.run(main())
