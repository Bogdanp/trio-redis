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
        if not data or data[0] not in known_prefixes:
            raise ProtocolError(f"Unexpected response from Redis: {data!r}.")

        elif data[0] == SP:
            return await self.process_simple_string(data)

        elif data[0] == EP:
            return await self.process_error_string(data)

        elif data[0] == IP:
            return int(await self.process_simple_string(data))

        elif data[0] == BP:
            return await self.process_bulk_string(data)

        elif data[0] == AP:
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
        length, data = extract_length(data)
        if length == -1:
            return None

        while len(data) < length + 2:
            data += await self.sock.recv(self.bufsize)

        return data[:-2]

    async def process_array(self, data):
        length, data = extract_length(data)
        if length == -1:
            return None

        items = []
        while len(items) < length:
            if not data:
                data += await self.sock.recv(self.bufsize)
                continue

            try:
                item, data = parse(data)
                items.append(item)
            except ReadMore:
                data += await self.sock.recv(self.bufsize)

        return items


class ReadMore(Exception):
    """Raised by parse to signal that it needs more data to parse.
    """


def parse(data):
    try:
        index = data.index(b"\r\n")
    except ValueError:
        raise ReadMore()

    if data[0] not in known_prefixes:
        raise ProtocolError(f"Unexpected data in array response: {data!r}.")

    elif data[0] == SP:
        return data[1:index], data[index + 2:]

    elif data[0] == IP:
        return int(data[1:index]), data[index + 2:]

    elif data[0] == BP:
        length, data = extract_length(data)
        if length == -1:
            return None

        elif len(data) < length + 2:
            raise ReadMore()

        return data[:length], data[length + 2:]

    elif data[0] == AP:
        length, data = extract_length(data)
        if length == -1:
            return None

        items = []
        while len(items) < length:
            item, data = parse(data)
            items.append(item)

        return items

    else:
        assert False, "unreachable"


def extract_length(data):
    for i, c in enumerate(data[1:], 1):
        if c == 13:
            return int(data[1:i]), data[i + 2:]
