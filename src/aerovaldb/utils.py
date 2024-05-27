import asyncio
import functools
from typing import Callable


def _has_async_loop():
    is_async = False
    try:
        loop = asyncio.get_running_loop()
        if loop is not None:
            is_async = True
    except RuntimeError:
        is_async = False
    return is_async


def async_and_sync(function: Callable) -> Callable:
    """Wrap an async method to a sync method.

    This allows to run the async method in both async and sync contexts transparently
    without any additional code.

    :args function: function/property to wrap
    :return: modified function
    """
    @functools.wraps(function)
    def async_and_sync_wrap(*args, **kwargs):
        if _has_async_loop():
            return function(*args, **kwargs)
        else:
            return asyncio.run(function(*args, **kwargs))

    return async_and_sync_wrap




