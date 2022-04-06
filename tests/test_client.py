"""
Test Client
"""

import pytest  # type: ignore
from redis import exceptions # type: ignore[attr-defined]

from tests.utils import TEST_URL  # type: ignore
from redismq import Client


@pytest.mark.asyncio  # type: ignore[misc]
async def test_client_connect() -> None:
    "test client with valid options"
    p_connection = await Client.connect(TEST_URL, namespace='foo')
    await p_connection.close()


@pytest.mark.asyncio  # type: ignore[misc]
async def test_client_barf() -> None:
    "test client with invalid URL should throw"
    with pytest.raises(exceptions.ConnectionError):
        await Client.connect("redis://bogus")


@pytest.mark.asyncio  # type: ignore[misc]
async def test_multiple_client_close() -> None:
    "multiple clients should open/close gracefully"
    p_connection = await Client.connect(TEST_URL)
    q_connection = await Client.connect(TEST_URL)
    await q_connection.close()
    await p_connection.close()


@pytest.mark.asyncio  # type: ignore[misc]
async def test_client_close_twice() -> None:
    "client closed twice should throw"
    p_connection = await Client.connect(TEST_URL)
    await p_connection.close()
    with pytest.raises(RuntimeError):
        await p_connection.close()
