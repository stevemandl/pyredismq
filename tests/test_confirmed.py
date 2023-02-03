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
            await payload.ack(f"Acknowledged {payload.message}")
        except Exception as err:
            print(f"ack_confirmed_messages exception: {err}")
            raise


@pytest.mark.asyncio  # type: ignore[misc]
async def test_send_and_read() -> None:
    "test sending a confirmed message and reading/confirming it"
    p_connection = await Client.connect(TEST_URL)
    await p_connection.redis.delete("mystream")
    my_producer = await p_connection.producer("mystream")
    # add a message so pending messages will get processed
    await my_producer.addUnconfirmedMessage("Hello there!")
    q_connection = await Client.connect(TEST_URL)
    my_consumer = await q_connection.consumer("mystream", "mygroup", "consumer1")

    send_task = asyncio.create_task(send_a_confirmed_message(my_producer))
    recv_task = asyncio.create_task(read_a_confirmed_message(my_consumer))
    await asyncio.gather(send_task, recv_task)
    recv_task.cancel()
    await p_connection.dispose_producer(my_producer)

    await p_connection.close()
    await q_connection.close()

@pytest.mark.asyncio  # type: ignore[misc]
async def test_pending() -> None:
    "test processing pending messages"
    p_connection = await Client.connect(TEST_URL)
    q_connection = await Client.connect(TEST_URL)
    await p_connection.redis.delete("mystream")
    my_producer = await p_connection.producer("mystream")
    await q_connection.consumer("mystream", "mygroup", "consumer0")
    # add a message 
    await my_producer.addUnconfirmedMessage("Hello there!")
    # xread the message so it looks like it was read by consumer1 but never processed
    await p_connection.redis.xreadgroup(groupname='mygroup', consumername='consumer1',count=1, block=500,streams={'mystream': b">"})
    await q_connection.consumer("mystream", "mygroup", "consumer1")
    await p_connection.dispose_producer(my_producer)

    await p_connection.close()
    await q_connection.close()


@pytest.mark.execution_timeout(10)
@pytest.mark.asyncio  # type: ignore[misc]
async def test_slow_send_and_read() -> None:
    "test sending a slow confirmed message and reading/confirming it"
    p_connection = await Client.connect(TEST_URL)
    await p_connection.redis.delete("mystream")
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
    await p_connection.redis.delete("mystream")
    my_producer = await p_connection.producer("mystream")

    q_connection = await Client.connect(TEST_URL)
    my_consumer = await q_connection.consumer("mystream", "mygroup", "consumer1")

    ack_task = asyncio.create_task(ack_confirmed_messages(my_consumer))
    for i in range(10):
        payload = f"message {i}"
        response = await my_producer.addConfirmedMessage(payload)
        assert response["message"] == f"Acknowledged {payload}"
    ack_task.cancel()

    await p_connection.close()
    await q_connection.close()

@pytest.mark.execution_timeout(20)
@pytest.mark.asyncio  # type: ignore[misc]
async def test_concurrent_confirmed() -> None:
    "test many concurrent confirmed messages"
    p_connection = await Client.connect(TEST_URL)
    await p_connection.redis.delete("mystream")
    my_producer = await p_connection.producer("mystream")

    q_connection = await Client.connect(TEST_URL)
    my_consumer = await q_connection.consumer("mystream", "mygroup", "consumer1")
    confirmed_coros = []
    for i in range(20):
        # response = await my_producer.addConfirmedMessage(f"message {i}")
        confirmed_coros.append( my_producer.addConfirmedMessage(f"message {i}"))
    ack_task = asyncio.create_task(ack_confirmed_messages(my_consumer))
    responses = await asyncio.gather(*confirmed_coros)
    assert all([responses[i]["message"] == f"Acknowledged message {i}" for i in range(20)])
    ack_task.cancel()

    await p_connection.close()
    await q_connection.close()
