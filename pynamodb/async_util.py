import functools


def run_secretly_sync_async_fn(async_fn, *args, **kwargs):
    # From https://github.com/python-trio/hip/issues/1#issuecomment-322028457
    coro = async_fn(*args, **kwargs)
    try:
        coro.send(None)
    except StopIteration as exc:
        return exc.value
    else:
        raise RuntimeError("you lied, this async function is not secretly synchronous")


def wrap_secretly_sync_async_fn(async_fn):
    @functools.wraps(async_fn)
    def wrap(*args, **kwargs):
        return run_secretly_sync_async_fn(async_fn, *args, **kwargs)
    return wrap
