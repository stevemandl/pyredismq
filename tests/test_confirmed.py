"""
Test Confirmed Messages
"""
import asyncio
import pytest  # type: ignore
from tests.utils import TEST_URL  # type: ignore

from redismq import Client, Producer, Consumer


@pytest.fixture
def event_loop():
    """
    This special event loop fixture overriding the default one in pytest-asyncio
    allows canceled tasks a chance to process the CancelledError that is thrown
    into the task, otherwise the tests will pass but it generates "Task was
    destroyed but it is pending!" messages when the loop is stopped.

    https://docs.python.org/3/library/asyncio-task.html#asyncio.Task.cancel
    """
    loop = asyncio.get_event_loop()
    yield loop
    loop.call_soon(loop.stop)
    loop.run_forever()


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


async def ack_confirmed_messages(my_consumer: Consumer) -> None:
    "ack confirmed messages"
    while True:
        try:
            payload = await my_consumer.read()
            await payload.ack("Acknowledged")
        except Exception as err:
            print(f"ack_confirmed_messages exception: {err}")
            raise


@pytest.mark.asyncio  # type: ignore[misc]
async def test_send_and_read() -> None:
    "test sending a confirmed message and reading/confirming it"
    p_connection = await Client.connect(TEST_URL)
    await p_connection.connection_pool.delete("mystream")
    my_producer = await p_connection.producer("mystream")
    q_connection = await Client.connect(TEST_URL)
    my_consumer = await q_connection.consumer("mystream", "mygroup", "consumer1")

    send_task = asyncio.create_task(send_a_confirmed_message(my_producer))
    recv_task = asyncio.create_task(read_a_confirmed_message(my_consumer))
    await asyncio.gather(send_task, recv_task)
    recv_task.cancel()

    await p_connection.close()
    await q_connection.close()


@pytest.mark.execution_timeout(10)
@pytest.mark.asyncio  # type: ignore[misc]
async def test_slow_send_and_read() -> None:
    "test sending a slow confirmed message and reading/confirming it"
    p_connection = await Client.connect(TEST_URL)
    await p_connection.connection_pool.delete("mystream")
    my_producer = await p_connection.producer("mystream")
    q_connection = await Client.connect(TEST_URL)
    my_consumer = await q_connection.consumer("mystream", "mygroup", "consumer1")

    send_task = asyncio.create_task(send_a_confirmed_message(my_producer, 5))
    recv_task = asyncio.create_task(read_a_confirmed_message(my_consumer))
    await asyncio.gather(send_task, recv_task)
    recv_task.cancel()

    await p_connection.close()
    await q_connection.close()


@pytest.mark.execution_timeout(20)
@pytest.mark.asyncio  # type: ignore[misc]
async def test_multiple_confirmed() -> None:
    "test many confirmed messages"
    p_connection = await Client.connect(TEST_URL)
    await p_connection.connection_pool.delete("mystream")
    my_producer = await p_connection.producer("mystream")

    q_connection = await Client.connect(TEST_URL)
    my_consumer = await q_connection.consumer("mystream", "mygroup", "consumer1")

    ack_task = asyncio.create_task(ack_confirmed_messages(my_consumer))
    for i in range(10):
        response = await my_producer.addConfirmedMessage(f"message {i}")
        assert response["message"] == "Acknowledged"
    ack_task.cancel()

    await p_connection.close()
    await q_connection.close()
