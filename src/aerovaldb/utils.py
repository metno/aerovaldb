import re
import asyncio
import functools
from typing import Callable, ParamSpec, TypeVar
import simplejson
from .routes import ALL_ROUTES
import urllib


def json_dumps_wrapper(obj, **kwargs) -> str:
    """
    Wrapper which calls simplejson.dumps with the correct options, known to work for objects
    returned by Pyaerocom.

    This ensures that nan values are serialized as null to be compliant with the json standard.
    """
    return simplejson.dumps(obj, ignore_nan=True, **kwargs)


def parse_formatted_string(template: str, s: str) -> dict:
    """Match s against a python format string, extracting the
    parameter values from the format string in a dictionary.

    Note
    ----
    Only supports {named_parameter} style format.
    """

    # First split on any keyword arguments, note that the names of keyword arguments will be in the
    # 1st, 3rd, ... positions in this list
    tokens = re.split(r"\{(.*?)\}", template)
    keywords = tokens[1::2]

    # Now replace keyword arguments with named groups matching them. We also escape between keyword
    # arguments so we support meta-characters there. Re-join tokens to form our regexp pattern
    tokens[1::2] = map("(?P<{}>.*)".format, keywords)
    tokens[0::2] = map(re.escape, tokens[0::2])
    pattern = "".join(tokens)

    # Use our pattern to match the given string, raise if it doesn't match
    matches = re.match(pattern, s)
    if not matches:
        raise Exception("Format string did not match")

    # Return a dict with all of our keywords and their values
    return {x: matches.group(x) for x in keywords}


def parse_uri(uri: str) -> tuple[str, dict[str, str], dict[str, str]]:
    """
    Parses an uri returning a tuple consisting of
    - The route against which it was matched.
    - Route args.
    - Additional kwargs.

    Parameters
    ----------
    uri :
        The uri to be parsed.
    """
    split = uri.split("?")

    for template in ALL_ROUTES:
        if len(split) == 1:
            try:
                route_args = parse_formatted_string(template, split[0])
            except Exception:
                continue
            else:
                return (template, route_args, dict())

        elif len(split) == 2:
            try:
                route_args = parse_formatted_string(template, split[0])
            except Exception:
                continue

            kwargs = urllib.parse.parse_qs(split[1])
            kwargs = {k: v[0] for k, v in kwargs.items()}

            return (template, route_args, kwargs)

    raise ValueError(f"URI {uri} is not a valid URI.")


# Workaround to ensure function signature of the decorated function is shown correctly
# Solution from here: https://stackoverflow.com/questions/74074580/how-to-avoid-losing-type-hinting-of-decorated-function
P = ParamSpec("P")
T = TypeVar("T")


def _has_async_loop():
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
        if _has_async_loop():
            return function(*args, **kwargs)
        else:
            return asyncio.run(function(*args, **kwargs))

    return async_and_sync_wrap
