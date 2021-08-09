import asyncio
import functools


def wrap_secretly_sync_async_fn(async_fn):
    @functools.wraps(async_fn)
    def wrap(*args, **kwargs):
        asyncio.run(async_fn(*args, **kwargs))
    return wrap
