from collections import namedtuple

#: Wrapper class for values that don't have to be quoted.
atom = namedtuple("atom", ("value",))

#: The set of characters that must be escaped before being sent as
#: Redis strings.
escapes = {
    ord(b"\0"): rb"\x00",
    ord(b"\a"): rb"\a",
    ord(b"\b"): rb"\b",
    ord(b"\f"): rb"\f",
    ord(b"\n"): rb"\n",
    ord(b"\r"): rb"\r",
    ord(b"\t"): rb"\t",
    ord(b"\v"): rb"\v",
    ord(b"\\"): rb"\\",
    ord(b'"'): rb'\"',
}


def serialize(x):
    """Serialize `x` so that it can safely be sent to Redis.

    Parameters:
      x(object): The value to serialize.

    Returns:
      bytes: The serialized value.
    """
    if isinstance(x, atom):
        return x.value
    elif isinstance(x, bytes):
        return quote(x)
    elif isinstance(x, str):
        return quote(x.encode("utf-8"))
    elif isinstance(x, (float, int)):
        return str(x).encode("ascii")
    else:
        return serialize(str(x))


def quote(bs):
    return b'"' + bytes(escape(bs)) + b'"'


def escape(bs):
    for c in bs:
        if c in escapes:
            yield from escapes[c]
        else:
            yield c
