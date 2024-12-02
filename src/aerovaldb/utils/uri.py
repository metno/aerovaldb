import regex as re
from ..routes import ALL_ROUTES
import urllib


def extract_substitutions(template: str):
    """
    For a python template string, extracts the names between curly brackets:

    For example 'blah blah {test} blah {test2}' returns ["test", "test2"]
    """
    return re.findall(r"\{([a-zA-Z-]*?)\}", template)


def parse_formatted_string(template: str, s: str):
    keywords = extract_substitutions(template)

    pattern = "(" + "|".join([re.escape("{" + k + "}") for k in keywords]) + ")"
    segments = re.split(pattern, template)

    result = {}
    while len(segments) > 0:
        token = segments[0]
        if token.startswith("{"):
            if s[0] == '"':
                extr = s.split('"')[1]
                s = s[(len(extr) + 2) :]
            else:
                if (len(segments) >= 2 and segments[1] != "") or (len(segments) > 2):
                    ls: list[str] = []
                    while True:
                        ls.append(s[len(ls)])
                        if s[len(ls) :].startswith(segments[1]):
                            break

                    extr = "".join(ls)
                    s = s[(len(extr)) :]
                else:
                    extr = s

            result[token.replace("{", "").replace("}", "")] = extr
        else:
            if not s.startswith(token):
                raise Exception("Format string did not match")
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
        if not v.startswith('"'):
            route_args[k] = f'"{v}"'

    uri = route.format(**route_args)
    if kwargs:
        queries = "&".join([f"{k}={v}" for k, v in kwargs.items()])
        uri = f"{uri}?{queries}"

    return uri
