# trio-redis

A [Redis][redis] client based on [Trio][trio].

## Example

``` python
import trio

from trio_redis import Redis


async def hello_world():
  async with Redis() as redis:
    await redis.set("some-key", "hello, world!")
    print(await redis.get("some-key"))

trio.run(hello_world)
```


[redis]: https://redis.io/
[trio]: https://github.com/python-trio/trio
