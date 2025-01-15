import re
import urllib

from ..routes import ALL_ROUTES

encode_chars = {"%": "%0", "/": "%1"}


def encode_arg(string: str):
    ls: list[str] = []
    prev = 0
    i = 0
    while i < len(string):
        if string[i] in encode_chars:
            ls.append(string[prev:i] + encode_chars.get(string[i]))  # type: ignore
            prev = i + 1
        i += 1

    ls.append(string[prev:])

    return "".join(ls)


def decode_arg(string: str):
    ls: list[str] = []
    prev = 0
    i = 0
    while i < len(string):
        if string[i] != "%":
            i += 1
            continue

        for k, v in encode_chars.items():
            if string[i : (i + 2)] == v:
                ls.append(string[prev:i] + k)
                i += 2
                prev = i
                break
    ls.append(string[prev:])

    return "".join(ls)


def extract_substitutions(template: str):
    """
    For a python template string, extracts the names between curly brackets:

    For example 'blah blah {test} blah {test2}' returns ["test", "test2"]
    """
    return re.findall(r"\{([a-zA-Z-]*?)\}", template)


def parse_formatted_string(
    template: str, string: str, *, force_split: list[str] | None = ["/"]
):
    """Parse formatted string. Meant to be the inverse of str.format()

    :param template: Template string.
    :param string: String to be matched.
    :param force_split: Optional list of single character strings which will force a break between tokens.
    :raises Exception: If unable to match `s` against template.
    :return: Dict of extracted arguments.

    Limitations
    -----------
    - Only works for format strings that use the named curly bracket notation.
    In other words no %s or {} notation.

    Example:
    >>> from aerovaldb.utils.uri import parse_formatted_string
    >>> parse_formatted_string("{a}/{b}", "test1/test2")
    {'a': 'test1', 'b': 'test2'}
    """
    if force_split is None:
        force_split = []

    if any([not isinstance(x, str) for x in force_split]):
        raise TypeError(
            f"force_split got elements that aren't string. Got {force_split}."
        )

    if any([len(x) != 1 for x in force_split]):
        raise ValueError(
            f"force_split must be a list of single character strings. Got {force_split}."
        )

    original_string = string
    keywords = extract_substitutions(template)

    pattern = "(" + "|".join([re.escape("{" + k + "}") for k in keywords]) + ")"
    segments = [x for x in re.split(pattern, template) if x != ""]
    # Segments is a list of constant strings and keywords (Keywords starting with '{').
    # For instance 'a{b}c{d}' -> ['a', '{b}', 'c', '{d}']

    result = {}
    while len(segments) > 0:
        token = segments[0]
        next_token = None
        if len(segments) >= 2:
            next_token = segments[1]
        if token.startswith("{"):
            # Token is a keyword, so try to extract it.
            ls: list[str] = []
            if next_token is not None:
                if next_token.startswith("{"):
                    raise Exception(
                        f"Two successive keywords can not be disambiguated (s='{original_string}; template='{template}')"
                    )

                # First opportunity where the remainder of the string starts with the next token is where we stop matching.
                # Note: This prevents some strings from being matched if the next token is also part of the string that should
                # be matched, but it isn't causing problems for now.
                while len(ls) < len(string) and not (
                    string[len(ls) :].startswith(next_token)
                ):
                    char = string[len(ls)]
                    if char in force_split:
                        break
                    ls.append(char)

                extr = "".join(ls)
            else:
                extr = string

            result[token.replace("{", "").replace("}", "")] = extr
            string = string[len(extr) :]
        else:
            if not string.startswith(token):
                break

            string = string[len(token) :]

        segments = segments[1:]
    if len(segments) > 0:
        raise Exception(
            f"Formatted string '{original_string}' did not match template string '{template}'"
        )
    return result


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

    Example
    -------
    >>> from aerovaldb.utils.uri import parse_uri
    >>> parse_uri('/v0/experiments/project')
    ('/v0/experiments/{project}', {'project': 'project'}, {})
    """
    split = uri.split("?")

    for template in ALL_ROUTES:
        if len(split) == 1:
            try:
                route_args = parse_formatted_string(template, split[0])
            except Exception:
                continue
            else:
                for k, v in route_args.items():
                    route_args[k] = v.replace(":", "/")
                return (template, route_args, dict())

        elif len(split) == 2:
            try:
                route_args = parse_formatted_string(template, split[0])
            except Exception:
                continue

            kwargs = urllib.parse.parse_qs(split[1])  # type: ignore
            kwargs = {k: v[0] for k, v in kwargs.items()}

            for k, v in route_args.items():
                route_args[k] = decode_arg(v)
            for k, v in kwargs.items():
                kwargs[k] = decode_arg(v)
            return (template, route_args, kwargs)

    raise ValueError(f"URI {uri} is not a valid URI.")


def build_uri(route: str, route_args: dict, kwargs: dict = {}) -> str:
    for k, v in route_args.items():
        route_args[k] = encode_arg(v)
    for k, v in kwargs.items():
        kwargs[k] = encode_arg(v)
    uri = route.format(**route_args)
    if kwargs:
        queries = "&".join([f"{k}={v}" for k, v in kwargs.items()])
        uri = f"{uri}?{queries}"

    return uri
