"""
IO Director

This sample application is a "confirming consumer" of requests for IO to
various points.  It looks up the object name (the 'objName' field in the
request), determines which "protocol" to use (the stream name of the servers
that will handle that protocol), and forwards the request.

This application takes the consumer name as a parameter, like 'director-a',
for running mutilple directors.
"""
from __future__ import annotations

import sys
import asyncio
import inspect
import json

from redismq.debugging import debugging
from redismq import Client, Consumer

mq: Client
consumer: Consumer


object_definitions = {
    "oat": {"name": "oat", "protocol": "protocol-a",},
    "rh": {"name": "rh", "protocol": "protocol-b",},
}


@debugging
async def dispatch(payload: Consumer.Payload) -> None:
    """
    Interpret the JSON encoded payload request and call the associated
    function.
    """
    dispatch.log_debug("dispatch %r", payload)  # type: ignore[attr-defined]
    global mq

    try:
        request = payload.message
        dispatch.log_debug("    - request: %r", request)  # type: ignore[attr-defined]
        if not isinstance(request, dict):
            raise TypeError("JSON object expected")

        # look for the function to call
        fn = request.get("fn", None)
        if fn is None:
            raise RuntimeError("missing 'fn'")
        if fn not in ("ioRead",):
            raise NameError(f"function {fn!r} is not defined")

        # look for the object
        object_name = request.get("objName", None)
        if object_name is None:
            raise RuntimeError("missing 'objName'")
        if object_name not in object_definitions:
            raise NameError(f"object {object_name!r} is not defined")

        # extract the protocol stream name from the definition
        protocol_stream_name = object_definitions[object_name]["protocol"]

        # create a producer for the stream
        producer = await mq.producer(protocol_stream_name)
        dispatch.log_debug("    - producer: %r", producer)

        # forward the request along, this will look like a confirmed service
        # request to the server
        await producer.addUnconfirmedMessage(
            message=request, response_channel_id=payload.response_channel,
        )

        # kill the response channel so no message goes back to the client
        payload.response_channel = None

        # half way to success
        await payload.ack(None)

    except Exception as err:
        return await payload.ack({"error": str(err)})


@debugging
async def main() -> None:
    """
    Main method
    """
    main.log_debug("starting...")  # type: ignore[attr-defined]
    global mq

    director_name = sys.argv[1]

    mq = await Client.connect("redis://localhost")
    consumer = await mq.consumer("testStream", "testGroup", director_name)
    while True:
        payload = await consumer.read()
        main.log_debug("    - payload: %r", payload)  # type: ignore[attr-defined]
        asyncio.create_task(dispatch(payload))


if __name__ == "__main__":
    asyncio.run(main())
