import asyncio
import functools
from typing import Callable, ParamSpec, TypeVar

# Workaround to ensure function signature of the decorated function is shown correctly
# Solution from here: https://stackoverflow.com/questions/74074580/how-to-avoid-losing-type-hinting-of-decorated-function
P = ParamSpec("P")
T = TypeVar("T")


def has_async_loop():
    is_async = False
    try:
        loop = asyncio.get_running_loop()
        if loop is not None:
            is_async = True
    except RuntimeError:
        is_async = False
    return is_async


def async_and_sync(function: Callable[P, T]) -> Callable[P, T]:
    """Wrap an async method to a sync method.

    This allows to run the async method in both async and sync contexts transparently
    without any additional code.

    :args function: function/property to wrap
    :return: modified function
    """

    @functools.wraps(function)
    def async_and_sync_wrap(*args, **kwargs):
        if has_async_loop():
            return function(*args, **kwargs)
        else:
            return asyncio.run(function(*args, **kwargs))

    return async_and_sync_wrap


@async_and_sync
async def run_until_finished(coroutine):
    """
    Takes a aio coroutine, runs it and waits for it to finish.

    :param coroutine : The coroutine to be ran.
    :return
        The result from running coroutine.
    """
    task = asyncio.create_task(coroutine)
    await asyncio.wait(task)
    return task.result()
