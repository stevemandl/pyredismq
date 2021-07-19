"""
test_confirmed.py
"""
import asyncio
import pytest  # type: ignore
from tests.utils import TEST_URL  # type: ignore

from redismq import Client, Producer, Consumer


async def send_a_confirmed_message(my_producer: Producer, delay: int = 0) -> None:
    "test confirmed message"
    await asyncio.sleep(delay)
    response = await my_producer.addConfirmedMessage(
        "Hello there! Let me know when you get this."
    )
    assert response["message"] == "I got your message"


async def read_a_confirmed_message(my_consumer: Consumer) -> None:
    "test reading a confirmed message"
    payload = await my_consumer.read()
    resp = "I got your message" if payload.response_channel else "no response"
    await payload.ack(resp)

async def close_a_consumer(my_consumer: Consumer) -> None:
    "close consumer"
    await asyncio.sleep(1)
    my_consumer.close()

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

    await p_connection.close()
    await q_connection.close()

@pytest.mark.timeout(10)
@pytest.mark.asyncio  # type: ignore[misc]
async def test_slow_send_and_read() -> None:
    "test sending a slow confirmed message and reading/confirming it"
    p_connection = await Client.connect(TEST_URL)
    await p_connection.redis.delete("mystream")
    my_producer = await p_connection.producer("mystream")
    q_connection = await Client.connect(TEST_URL)
    my_consumer = await q_connection.consumer("mystream", "mygroup", "consumer1")
    my_consumer.xread_timeout = 1

    send_task = asyncio.create_task(send_a_confirmed_message(my_producer, 5))
    recv_task = asyncio.create_task(read_a_confirmed_message(my_consumer))
    await asyncio.gather(send_task, recv_task)
    send_task.done()
    recv_task.cancel()

    await p_connection.close()
    await q_connection.close()

@pytest.mark.asyncio  # type: ignore[misc]
async def test_close_consumer() -> None:
    "test close consumer"
    mq_connection = await Client.connect(TEST_URL)
    my_consumer = await mq_connection.consumer("mystream", "mygroup", "consumer1")
    recv_task = asyncio.create_task(read_a_confirmed_message(my_consumer))
    close_task = asyncio.create_task(close_a_consumer(my_consumer))
    await asyncio.gather(recv_task, close_task)
    await mq_connection.close()
