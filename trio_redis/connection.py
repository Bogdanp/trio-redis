import trio

from .errors import ProtocolError, ResponseError, ResponseTypeError
from .serialization import atom, serialize

_simple_prefix = ord("+")
_error_prefix = ord("-")
_integer_prefix = ord(":")
_bulk_prefix = ord("$")
_array_prefix = ord("*")

#: The set of known Redis response prefixes.
_known_prefixes = {_simple_prefix, _error_prefix, _integer_prefix, _bulk_prefix, _array_prefix}


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
        if not data or data[0] not in _known_prefixes:
            raise ProtocolError(f"Unexpected response from Redis: {data!r}.")

        elif data[0] == _simple_prefix:
            return await self.process_simple_string(data)

        elif data[0] == _error_prefix:
            return await self.process_error_string(data)

        elif data[0] == _integer_prefix:
            return int(await self.process_simple_string(data))

        elif data[0] == _bulk_prefix:
            return await self.process_bulk_string(data)

        elif data[0] == _array_prefix:
            return await self.process_array(data)

        else:
            assert False, "unreachable"

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
