"""
Test Multi-channel PubSub
"""
import pytest  # type: ignore

from redismq import Client
from tests.utils import TEST_URL  # type: ignore


@pytest.mark.asyncio  # type: ignore[misc]
async def test_one_to_one_001() -> None:
    """
    test a message from a single publisher to a single subscriber on one channel
    """
    client = await Client.connect(TEST_URL)
    publisher = await client.publisher(["topic1"])
    subscriber = await client.subscriber(["topic1"])

    # publish the message
    await publisher.publish("hi there")

    # get and acknowledge the message
    payload = await subscriber.read()
    assert payload.message == "hi there"
    await payload.ack()

    await client.close()


@pytest.mark.asyncio  # type: ignore[misc]
async def test_one_to_many_002() -> None:
    """
    test a message from a single publisher to a multiple subscribers on one
    channel
    """
    client = await Client.connect(TEST_URL)
    publisher = await client.publisher(["topic1"])
    subscriber1 = await client.subscriber(["topic1"])
    subscriber2 = await client.subscriber(["topic1"])

    # publish the message
    await publisher.publish("hi there")

    # get and acknowledge the message
    for subscriber in (subscriber1, subscriber2):
        payload = await subscriber.read()
        await payload.ack()

    await client.close()


@pytest.mark.asyncio  # type: ignore[misc]
async def test_one_to_many_003() -> None:
    """
    test a message from a single publisher to a multiple subscribers on
    different channels
    """
    client = await Client.connect(TEST_URL)
    publisher = await client.publisher(["topic1", "topic2"])
    subscriber1 = await client.subscriber(["topic1"])
    subscriber2 = await client.subscriber(["topic2"])

    # publish the message
    await publisher.publish("hi there")

    # get and acknowledge the message
    for subscriber in (subscriber1, subscriber2):
        payload = await subscriber.read()
        await payload.ack()

    await client.close()


@pytest.mark.asyncio  # type: ignore[misc]
async def test_one_to_one_004() -> None:
    """
    test a message from a single publisher to a subscriber on multiple channels
    passing the channel names as a parameter
    """
    client = await Client.connect(TEST_URL)
    publisher = await client.publisher()
    subscriber = await client.subscriber(["topic1", "topic2"])

    # publish the message
    await publisher.publish("hi there", ["topic1", "topic2"])

    # get and acknowledge the messages
    payload1 = await subscriber.read()
    payload2 = await subscriber.read()
    await payload1.ack()
    await payload2.ack()

    await client.close()
