import trio

from .errors import ProtocolError, ResponseError, ResponseTypeError
from .serialization import atom, serialize

SP = ord("+")
EP = ord("-")
IP = ord(":")
BP = ord("$")
AP = ord("*")

#: The set of known Redis response prefixes.
known_prefixes = {SP, EP, IP, BP, AP}


class ReadMore(Exception):
    """Raised by parse to signal that it needs more data.
    """


class RedisConnection:
    """This class faciliates all communication with Redis via a trio socket.

    Warning:
      The interface of this class may change at any time, without notice.

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
        while True:
            try:
                item, _ = await self.parse(data)
                return item
            except ReadMore:
                data += await self.sock.recv(self.bufsize)

    async def parse(self, data):
        try:
            index = data.index(b"\r\n")
        except ValueError:
            raise ReadMore()

        if data[0] not in known_prefixes:
            raise ProtocolError(f"Unexpected data in response: {data!r}.")

        elif data[0] == SP:
            return data[1:index], data[index + 2:]

        elif data[0] == EP:
            error = data[1:index].decode("ascii")
            if error.startswith("WRONGTYPE"):
                raise ResponseTypeError(error[len("WRONGTYPE "):])

            elif error.startswith("ERR"):
                raise ResponseError(error[len("ERR "):])

            else:
                raise ResponseError(error)

        elif data[0] == IP:
            return int(data[1:index]), data[index + 2:]

        elif data[0] == BP:
            length, data = int(data[1:index]), data[index + 2:]
            if length == -1:
                return None, data

            elif len(data) < length + 2:
                raise ReadMore()

            return data[:length], data[length + 2:]

        elif data[0] == AP:
            length, data = int(data[1:index]), data[index + 2:]
            if length == -1:
                return None, data

            return await self.parse_array(length, data)

        else:  # pragma: no cover
            assert False, "unreachable"

    async def parse_array(self, length, data):
        items = []
        while len(items) < length:
            if not data:
                data += await self.sock.recv(self.bufsize)
                continue

            try:
                item, data = await self.parse(data)
                items.append(item)
            except ReadMore:
                data += await self.sock.recv(self.bufsize)

        return items, data
