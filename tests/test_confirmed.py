"""
Test Confirmed Messages
"""
import asyncio
import pytest  # type: ignore
from tests.utils import TEST_URL  # type: ignore
from redismq.debugging import debugging
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


async def botch_a_confirmed_message(my_conn, my_stream, my_group, my_consumer) -> None:
    "return bogus response to a confirmed message"

    args = {
        "groupname": my_group,
        "consumername": my_consumer,
        "count": 1,
        "block": 1000,
        "streams": {my_stream: b">"},
    }
    while True:
        messages = await my_conn.xreadgroup(**args)
        if not messages:
            continue
        stream, element_list = messages[0]
        msg_id, payload = element_list[0]
        payload_dict = dict(payload)
        response_channel = payload_dict.get("response_channel", None)
        # ack the message in the stream
        await my_conn.xack(my_stream, my_group, msg_id)
        if response_channel is not None:
            await my_conn.publish(response_channel, "not json")
            return


async def receive_a_botched_confirmation(my_producer: Producer, delay: int = 0) -> None:
    "test botched confirmation"
    await asyncio.sleep(delay)
    response = await my_producer.addConfirmedMessage(
        "Hello there! Let me know when you get this."
    )
    assert response["message"] == "JSON Decoding Error"
    assert "err" in response


async def ack_confirmed_messages(my_consumer: Consumer) -> None:
    "ack confirmed messages"
    while True:
        try:
            payload = await my_consumer.read()
            await payload.ack(f"Acknowledged {payload.message}")
        except Exception as err:
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
    await p_connection.redis.xreadgroup(
        groupname="mygroup",
        consumername="consumer1",
        count=1,
        block=500,
        streams={"mystream": b">"},
    )
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
        confirmed_coros.append(my_producer.addConfirmedMessage(f"message {i}"))
    ack_task = asyncio.create_task(ack_confirmed_messages(my_consumer))
    responses = await asyncio.gather(*confirmed_coros)
    assert all(
        [responses[i]["message"] == f"Acknowledged message {i}" for i in range(20)]
    )
    ack_task.cancel()

    await p_connection.close()
    await q_connection.close()


@pytest.mark.asyncio  # type: ignore[misc]
async def test_bad_json() -> None:
    "test sending a confirmed message receiving a bogus response"
    p_connection = await Client.connect(TEST_URL)
    await p_connection.redis.delete("mystream")
    my_producer = await p_connection.producer("mystream")
    q_connection = await Client.connect(TEST_URL)
    # call client.consumer just to create the consumer group
    await q_connection.consumer("mystream", "mygroup", "consumer1")

    # add a message
    await my_producer.addUnconfirmedMessage("Hello there!")

    send_task = asyncio.create_task(receive_a_botched_confirmation(my_producer))
    recv_task = asyncio.create_task(
        botch_a_confirmed_message(
            q_connection.redis, "mystream", "mygroup", "consumer1"
        )
    )
    await asyncio.gather(send_task, recv_task)
    recv_task.cancel()
    await p_connection.dispose_producer(my_producer)

    await p_connection.close()
    await q_connection.close()


@pytest.mark.asyncio  # type: ignore[misc]
async def test_confirmed_timeout() -> None:
    "test timeout confirmed message"
    p_connection = await Client.connect(TEST_URL)
    await p_connection.redis.delete("mystream")
    my_producer = await p_connection.producer("mystream", timeout=0.1)
    # no consumer
    resp = await my_producer.addConfirmedMessage(f"message to nowhere")
    assert resp["message"] == "Timeout Error"
    assert "err" in resp
    # commented out because redis does not reliably report no subscribers
    # even though the producer unsubscribed
    #    channels = await p_connection.redis.pubsub_channels()
    #    for channel in channels:
    # to force redis to update the channel list for this connection
    #        await p_connection.redis.publish(channel, "blah")
    #        assert (await p_connection.redis.execute_command('PUBSUB', 'NUMSUB', channel))[1] == 0
    await p_connection.close()


@debugging
@pytest.mark.asyncio  # type: ignore[misc]
async def test_cancelled_confirmed() -> None:
    "test cancelling a confirmed message"
    p_connection = await Client.connect(TEST_URL)
    await p_connection.redis.delete("mystream")
    my_producer = await p_connection.producer("mystream")
    # no consumer
    coro = my_producer.addConfirmedMessage("msg")
    try:
        await asyncio.wait_for(coro, timeout=0.01)
    except asyncio.TimeoutError:
        pass
    # commented out because redis does not reliably report no subscribers
    # even though the producer unsubscribed
    #        channels = await p_connection.redis.pubsub_channels()
    #        while channels:
    #            print(f"addConfirmed running: {coro.cr_running} channels: {channels}")
    #            await asyncio.sleep(.01)
    #            channels = await p_connection.redis.pubsub_channels()
    await p_connection.close()
