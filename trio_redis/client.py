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

    def close(self):
        """Close the connection to the Redis server.
        """
        self.conn.close()

    async def flushall(self):
        return await self.conn.process_command_ok(b"FLUSHALL")

    async def set(self, key, value):
        return await self.conn.process_command_ok(b"SET", key, value)

    async def get(self, key):
        return await self.conn.process_command(b"GET", key)

    async def delete(self, *keys):
        return await self.conn.process_command(b"DEL", *keys)

    async def __aenter__(self):
        return await self.connect()

    async def __aexit__(self, exc_type, exc_value, traceback):
        self.close()
