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

    keywords = ["{" + k + "}" for k in keywords]
    # keywords.extend([""] * (len(segments)-len(keywords)))
    # weaved = [item for pair in zip(segments, keywords) for item in pair]
    weaved = segments
    result = {}
    while len(weaved) > 0:
        token = weaved[0]
        if token.startswith("{"):
            if s[0] == '"':
                extr = s.split('"')[1]
                s = s[(len(extr) + 2) :]
            else:
                if (len(weaved) >= 2 and weaved[1] != "") or (len(weaved) > 2):
                    ls: list[str] = []
                    while True:
                        ls.append(s[len(ls)])
                        if s[len(ls) :].startswith(weaved[1]):
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
            # template = template[len(token):]

        weaved = weaved[1:]
    return result


# def parse_formatted_string(template: str, s: str) -> dict:
#    """Match s against a python format string, extracting the
#    parameter values from the format string in a dictionary.
#
#    Note
#    ----
#    Only supports {named_parameter} style format.
#    """
#
#    # First split on any keyword arguments, note that the names of keyword arguments will be in the
#    # 1st, 3rd, ... positions in this list
#    tokens = re.split(r"\{([a-zA-Z-]*?)\}", template)
#    # keywords = tokens[1::2]
#    keywords = extract_substitutions(template)
#    # Now replace keyword arguments with named groups matching them. We also escape between keyword
#    # arguments so we support meta-characters there. Re-join tokens to form our regexp pattern
#
#    tokens[1::2] = map("(?P<{}>[^/]*)".format, keywords)
#    tokens[0::2] = map(re.escape, tokens[0::2])
#    pattern = "".join(tokens)
#
#    # Use our pattern to match the given string, raise if it doesn't match
#    if not (match := re.match(pattern, s)):
#        raise Exception("Format string did not match")
#
#    # Return a dict with all of our keywords and their values
#    return {x: match.group(x) for x in keywords}


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
