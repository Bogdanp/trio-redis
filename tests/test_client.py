import os
import pytest

from trio_redis import Redis, ResponseTypeError


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
        await self.client.close()


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


async def test_delete_many():
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


async def test_echo():
    # Given a Redis connection
    async with redis() as client:
        message = b"Yes, this is dog."
        # If I echo a message,
        # I expect to get it back.
        assert await client.echo(message) == message


async def test_hget():
    # Given a Redis connection
    async with redis() as client:
        # If I set a hash field
        assert await client.hset("some-hash", "foo", 42) == 1

        # Then try to get it,
        # I expect to get back the value I set
        assert await client.hget("some-hash", "foo") == b"42"

        # If I try to get a field from something that's not a hash,
        # I expect to get back None.
        assert await client.hget("not-a-hash", "bar") is None


async def test_hgetall():
    # Given a Redis connection
    async with redis() as client:
        # If I do a multi-set on a dict
        data = {b"a": b"1", b"b": b"c"}
        assert await client.hmset("some-hash", data)

        # Then try to get it,
        # I expect to get back the same dict
        assert await client.hgetall("some-hash") == data


async def test_lrange():
    # Given a Redis connection
    async with redis() as client:
        values = [b"a", b"b", b"c", b"d"]

        # If I push values to a list
        assert await client.rpush("some-list", *values) == 4

        # And then I retrieve a range of those values,
        # I expect to get back exactly that range
        assert await client.lrange("some-list", -1, 0) == []
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


@pytest.mark.parametrize("values", [
    ["hello"],
    ["hello", "world!"],
])
async def test_rpush(values):
    # Given a Redis connection
    async with redis() as client:
        # If I push values to a list, I expect to get back the number of values added
        assert await client.rpush("some-list", *values) == len(values)

        # If I push a value into a key that's not a list,
        # I expect to get a type error back
        assert await client.set("not-a-list", 1)
        with pytest.raises(ResponseTypeError):
            assert await client.rpush("not-a-list", 2)


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


@pytest.mark.parametrize("string", blns)
async def test_set_and_retrieve_all_sorts_of_values(string):
    # Given a Redis connection
    async with redis() as client:
        # If I set a naughty string
        assert await client.set("naughty", string)
        # Then get it back, I expect it to have the same value
        assert await client.get("naughty") == string
