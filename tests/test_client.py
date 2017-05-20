import os
import pytest

from trio_redis import Redis


def rel(*xs):
    return os.path.join(os.path.abspath(os.path.dirname(__file__)), *xs)


with open(rel("blns.txt"), "rb") as f:
    blns = [line for line in f.readlines() if line and not line.startswith(b"#")]


class redis:
    def __init__(self):
        self.client = Redis()

    async def __aenter__(self):
        await self.client.connect()
        await self.client.flushall()
        return self.client

    async def __aexit__(self, exc_type, exc_info, traceback):
        self.client.close()


async def test_client_can_set_keys():
    # If I set a key, I expect a successful response
    async with redis() as client:
        assert await client.set("foo", 42)


async def test_client_can_get_keys():
    # Given a Redis connection
    async with redis() as client:
        # If I set a key
        assert await client.set("foo", 42)

        # And then get it,
        # I expect to receive the value that I set as a byte string
        assert await client.get("foo") == b"42"


async def test_client_can_get_missing_keys():
    # Given a Redis connection
    async with redis() as client:
        # If I try to get a missing key,
        # I expect to get nothing back
        assert await client.get("foo") is None


async def test_client_can_delete_keys():
    # Given a Redis connection
    async with redis() as client:
        # If I set a key
        assert await client.set("foo", 1)

        # And then delete it
        assert await client.delete("foo") == 1

        # If I then try to get it,
        # I expect to get nothing back
        assert await client.get("foo") is None


async def test_client_can_delete_many():
    # Given a Redis connection
    async with redis() as client:
        # If I set many keys
        keys = []
        for i in range(10):
            name = f"key-{i}"
            keys.append(name)
            assert await client.set(name, 1)

        # I expect to be able to delete them all using one call
        assert await client.delete(*keys) == len(keys)


@pytest.mark.parametrize("string", blns)
async def test_client_can_set_and_retrieve_all_sorts_of_values(string):
    # Given a Redis connection
    async with redis() as client:
        # If I set a naughty string
        assert await client.set("naughty", string)
        # Then get it back, I expect it to have the same value
        assert await client.get("naughty") == string
