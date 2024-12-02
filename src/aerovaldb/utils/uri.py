import re
from ..routes import ALL_ROUTES
import urllib


def extract_substitutions(template: str):
    """
    For a python template string, extracts the names between curly brackets:

    For example 'blah blah {test} blah {test2}' returns ["test", "test2"]
    """
    return re.findall(r"\{([a-zA-Z-]*?)\}", template)


def parse_formatted_string(template: str, s: str):
    """Parse formatted string. Meant to be the inverse of str.format()

    :param template: Template string.
    :param s: String to be matched.
    :raises Exception: If unable to match `s` against template.
    :return: Dict of extracted arguments.

    Limitations
    -----------
    - Only works for format strings that use the named curly bracket notation.
    In other words no %s or {}

    Note
    ----
    To allow for resolving ambiguous cases this function allows making boundaries
    explicit by using quotes.
    For instance:
    - template: "{a}{b}"  s: '"a""bcd" -> {'a': 'a', 'b': 'bcd'}
    """
    original_string = s
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
            if s[0] == '"':
                # Part to extract is delineated by quotes. Keep part until next unescaped quote
                # as the extracted value.
                quote_mode = True
            else:
                # Token is not delineated by quotes.
                # All text will be matched until the start of the unmatched part of `s` matches
                # the next segment.
                quote_mode = False

            ls: list[str] = []
            esc_flag = False
            offset = 0
            if not quote_mode and next_token is not None and next_token.startswith("{"):
                raise Exception(
                    f"Two successive keywords can not be disambiguated unless quotes are used (s='{original_string}; template='{template}')"
                )
            if len(segments) >= 2:
                esc_flag = False
                while True:
                    char = s[len(ls) + quote_mode + offset]
                    if not esc_flag and quote_mode and char == '"':
                        # End of extracted value in quote mode.
                        break
                    if esc_flag == True:
                        # Esc flag so append character no matter what it is.
                        ls.append(char)
                        esc_flag = False
                        continue
                    if char == "\\":
                        # Escape character, so mark esc_flag and ignore.
                        esc_flag = True
                        offset += 1
                        continue
                    ls.append(char)

                    if (
                        not quote_mode
                        and next_token is not None
                        and s[(len(ls) + quote_mode + offset) :].startswith(next_token)
                    ):
                        # Remainder of string matches next segment, quit early.
                        break
                extr = "".join(ls)
                s = s[(len(extr) + 2 * quote_mode + offset) :]
            else:
                if quote_mode and s.endswith('"'):
                    extr = s[1:-1]
                elif not quote_mode:
                    extr = s
                else:
                    raise Exception(
                        f"Formatted string '{original_string}' did not match template string '{template}'"
                    )

            result[token.replace("{", "").replace("}", "")] = extr
        else:
            if not s.startswith(token):
                raise Exception(
                    f"Formatted string '{original_string}' did not match template string '{template}'"
                )
            s = s[len(token) :]

        segments = segments[1:]
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
                route_args[k] = v.replace(":", "/")
            for k, v in kwargs.items():
                kwargs[k] = v.replace(":", "/")
            return (template, route_args, kwargs)

    raise ValueError(f"URI {uri} is not a valid URI.")


def build_uri(route: str, route_args: dict, kwargs: dict = {}) -> str:
    for k, v in route_args.items():
        encoded = v.replace('"', '\\"')
        if not v.startswith('"'):
            route_args[k] = f'"{encoded}"'

    uri = route.format(**route_args)
    if kwargs:
        queries = "&".join([f"{k}={v}" for k, v in kwargs.items()])
        uri = f"{uri}?{queries}"

    return uri
