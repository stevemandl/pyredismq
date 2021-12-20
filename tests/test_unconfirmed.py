"""
Test Unconfirmed Messages
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
    await mq_connection.close()


@pytest.mark.asyncio  # type: ignore[misc]
async def test_same_producer_twice() -> None:
    "test same producer"
    mq_connection = await Client.connect(TEST_URL)
    my_producer = await mq_connection.producer("mystream")
    your_producer = await mq_connection.producer("mystream")
    assert my_producer is your_producer
    await mq_connection.close()
