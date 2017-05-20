class RedisError(Exception):
    """Base class for all Redis-related errors.
    """


class ProtocolError(RedisError):
    """Raised when Redis responds with something that doesn't conform
    to the protocol.
    """


class ResponseError(RedisError):
    """Raised when Redis returns an error response.
    """


class ResponseTypeError(ResponseError):
    """Raised when Redis returns an error response with a `WRONGTYPE` prefix.
    """
