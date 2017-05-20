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


async def test_set():
    # If I set a key, I expect a successful response
    async with redis() as client:
        assert await client.set("foo", 42)


async def test_get():
    # Given a Redis connection
    async with redis() as client:
        # If I set a key
        assert await client.set("foo", 42)

        # And then get it,
        # I expect to receive the value that I set as a byte string
        assert await client.get("foo") == b"42"

        # If I try to get a missing key,
        # I expect to get nothing back.
        assert await client.get("bar") is None


async def test_delete():
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


@pytest.mark.parametrize("values", [
    ["hello"],
    ["hello", "world!"],
])
async def test_rpush(values):
    # Given a Redis connection
    async with redis() as client:
        # If I push values to a list, I expect to get back the number of values added
        assert await client.rpush("some-list", *values) == len(values)


async def test_rpushx():
    # Given a Redis connection
    async with redis() as client:
        # If I rpush values to a list
        assert await client.rpush("some-list", 1) == 1

        # Then rpushx a value into a that list,
        # I expect the value to get added to the list
        assert await client.rpushx("some-list", 2) == 2

        # If I rpushx a value into a key that's not a list,
        # I expect the operation to be a no-op
        assert await client.rpushx("some-key-thats-not-a-list", 2) == 0


async def test_lrange():
    # Given a Redis connection
    async with redis() as client:
        values = [b"a", b"b", b"c", b"d"]

        # If I push values to a list
        assert await client.rpush("some-list", *values) == 4

        # And then I retrieve a range of those values,
        # I expect to get back exactly that range
        assert await client.lrange("some-list", 0, -1) == values
        assert await client.lrange("some-list", 1, 1) == values[1:2]
        assert await client.lrange("some-list", 2, 3) == values[2:4]


async def test_lrange_with_long_list_of_values():
    # Given a Redis connection
    async with redis() as client:
        values = [b"a"] * (16 * 1024)

        # If I push values to a list
        assert await client.rpush("some-list", *values) == len(values)

        # And then I retrieve a range of those values,
        # I expect to get back exactly that range
        assert await client.lrange("some-list", 0, -1) == values


async def test_lindex():
    # Given a Redis connection
    async with redis() as client:
        values = [str(i).encode("ascii") for i in range(20)]

        # If I push values to a list
        assert await client.rpush("some-list", *values) == len(values)

        # And then I retrieve a a value at some index,
        # I expect to get back exactly that value
        for i, v in enumerate(values):
            assert await client.lindex("some-list", i) == v


@pytest.mark.parametrize("string", blns)
async def test_set_and_retrieve_all_sorts_of_values(string):
    # Given a Redis connection
    async with redis() as client:
        # If I set a naughty string
        assert await client.set("naughty", string)
        # Then get it back, I expect it to have the same value
        assert await client.get("naughty") == string
