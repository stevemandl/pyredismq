"""
test1.py
"""
import pytest  # type: ignore

import asyncio
from tests.utils import TEST_URL  # type: ignore

from redismq import Client, Producer, Consumer


async def send_a_confirmed_message(my_producer: Producer) -> None:
    "test confirmed message"
    response = await my_producer.addConfirmedMessage(
        "Hello there! Let me know when you get this."
    )
    assert response["message"] == "I got your message"


async def read_a_confirmed_message(my_consumer: Consumer) -> None:
    "test reading a confirmed message"
    payload = await my_consumer.read()
    resp = "I got your message" if payload.response_channel else "no response"
    await payload.ack(resp)


@pytest.mark.asyncio  # type: ignore[misc]
async def test_send_and_read() -> None:
    "test sending a confirmed message and reading/confirming it"
    p_connection = await Client.connect(TEST_URL)
    await p_connection.redis.delete("mystream")
    my_producer = await p_connection.producer("mystream")
    q_connection = await Client.connect(TEST_URL)
    my_consumer = await q_connection.consumer("mystream", "mygroup", "consumer1")

    send_task = asyncio.create_task(send_a_confirmed_message(my_producer))
    recv_task = asyncio.create_task(read_a_confirmed_message(my_consumer))
    await asyncio.gather(send_task, recv_task)
    send_task.done()
    recv_task.cancel()

    p_connection.redis.close()
    await p_connection.redis.wait_closed()

    q_connection.redis.close()
    await q_connection.redis.wait_closed()
