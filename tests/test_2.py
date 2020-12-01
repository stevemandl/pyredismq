"""
test2.py
"""
import pytest  # type: ignore

from redismq import Client
from tests.utils import TEST_URL  # type: ignore


@pytest.mark.asyncio  # type: ignore[misc]
async def test_send_an_unconfirmed_message() -> None:
    "test unconfirmed message"
    mq_connection = await Client.connect(TEST_URL)
    my_producer = await mq_connection.producer("mystream")

    response = await my_producer.addUnconfirmedMessage("Hello there!")
    print("Got message ID", response)

    mq_connection.redis.close()
    await mq_connection.redis.wait_closed()
