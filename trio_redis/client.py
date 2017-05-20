from .connection import RedisConnection


class Redis:
    """A Redis client.

    Parameters:
      addr(str): The IP address the Redis server is listening on.
      port(int): The port the Redis server is listening on.

    Examples:

      >>> async with Redis() as redis:
      ...   await redis.set("foo", 42)
      ...   await redis.get("foo")
      b'42'
    """

    def __init__(self, addr=b"127.0.0.1", port=6379):
        self.conn = RedisConnection(addr, port)

    async def connect(self):
        """Open a connection to the Redis server.

        Returns:
          Redis: This instance.
        """
        await self.conn.connect()
        return self

    async def close(self):
        """Close the connection to the Redis server.
        """
        await self.quit()
        self.conn.close()

    async def append(self, key, value):
        return await self.conn.process_command(b"APPEND", key, value)

    async def auth(self, password):
        return await self.con.process_command_ok(b"AUTH", password)

    async def delete(self, *keys):
        return await self.conn.process_command(b"DEL", *keys)

    async def echo(self, message):
        return await self.conn.process_command(b"ECHO", message)

    async def flushall(self):
        return await self.conn.process_command_ok(b"FLUSHALL")

    async def get(self, key):
        return await self.conn.process_command(b"GET", key)

    async def hget(self, key, field):
        return await self.conn.process_command(b"HGET", key, field)

    async def hgetall(self, key):
        items = await self.conn.process_command(b"HGETALL", key)
        return {items[i]: items[i + 1] for i in range(0, len(items), 2)}

    async def hmset(self, key, mapping):
        return await self.conn.process_command(b"HMSET", key, *(v for es in mapping.items() for v in es))

    async def hset(self, key, field, value):
        return await self.conn.process_command(b"HSET", key, field, value)

    async def lindex(self, key, index):
        return await self.conn.process_command(b"LINDEX", key, index)

    async def lpush(self, key, *values):
        return await self.conn.process_command(b"LPUSH", key, *values)

    async def lpushx(self, key, value):
        return await self.conn.process_command(b"LPUSHX", key, value)

    async def lrange(self, key, start, stop):
        return await self.conn.process_command(b"LRANGE", key, start, stop)

    async def rpush(self, key, *values):
        return await self.conn.process_command(b"RPUSH", key, *values)

    async def rpushx(self, key, value):
        return await self.conn.process_command(b"RPUSHX", key, value)

    async def quit(self):
        return await self.conn.process_command(b"QUIT")

    async def set(self, key, value):
        return await self.conn.process_command_ok(b"SET", key, value)

    async def __aenter__(self):
        return await self.connect()

    async def __aexit__(self, exc_type, exc_value, traceback):
        self.close()
