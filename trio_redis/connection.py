import trio

from .errors import ResponseError, ResponseTypeError
from .serialization import atom, serialize


class RedisConnection:
    """This class faciliates all communication with Redis via a trio socket.

    Parameters:
      addr(bytes)
      port(int)
      bufsize(int)
    """

    def __init__(self, addr, port, bufsize=16384):
        self.addr = (addr, port)
        self.sock = trio.socket.socket()
        self.bufsize = bufsize

    async def connect(self):
        await self.sock.connect(self.addr)

    def close(self):
        self.sock.close()

    async def send_command(self, command, *args):
        command_and_args = (serialize(arg) for arg in (atom(command),) + args)
        data = b" ".join(command_and_args) + b"\r\n"
        await self.sock.sendall(data)

    async def process_command(self, *command_and_args):
        await self.send_command(*command_and_args)
        return await self.process_response()

    async def process_command_ok(self, *command_and_args):
        await self.send_command(*command_and_args)
        return await self.process_response() == b"OK"

    async def process_response(self):
        data = await self.sock.recv(self.bufsize)
        if data.startswith(b"+"):
            return await self.process_simple_string(data)

        elif data.startswith(b"-"):
            return await self.process_error_string(data)

        elif data.startswith(b":"):
            return await self.process_integer(data)

        elif data.startswith(b"$"):
            return await self.process_bulk_string(data)

        else:
            raise ValueError("Unexpected data from Redis: {!r}".format(data))

    async def process_simple_string(self, data):
        while not data.endswith(b"\r\n"):
            data += await self.sock.recv(self.bufsize)

        return data[1:-2]

    async def process_error_string(self, data):
        error = (await self.process_simple_string(data)).decode("ascii")
        if error.startswith("WRONGTYPE"):
            raise ResponseTypeError(error[len("WRONGTYPE "):])

        elif error.startswith("ERR"):
            raise ResponseError(error[len("ERR "):])

        else:
            raise ResponseError(error)

    async def process_integer(self, data):
        return int(await self.process_simple_string(data))

    async def process_bulk_string(self, data):
        for i, c in enumerate(data[1:], 1):
            if c == 13:
                n, data = int(data[1:i]), data[i + 2:]
                break

        if n == -1:
            return None

        while len(data) < n + 2:
            data += await self.sock.recv(self.bufsize)

        return data[:-2]
