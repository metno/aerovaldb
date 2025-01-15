import re

PATH_COMPONENT_PATTERN = re.compile(r"^[^/]+$", flags=re.UNICODE)


def str_to_bool(value: str, /, default: bool | None = None) -> bool:
    """
    Parses a string as a boolean, supporting various values. It is intended
    mainly for parsing environment variables.

    Supports 1/0, true/false, t/f, yes/no, y/n (Case insensitive).

    :param value : The string value to be converted.
    :param default : Optional default return value (True/False) if the string
        value doesn't match any supported value. If not set, or set to None,
        a ValueError is raised in these cases.

    :raises ValueError
        If value is not a string
    :raises ValueError
        Raised on unsupported input value. Suppressed if default is set to True/False.

    >>> from aerovaldb.utils.string_utils import str_to_bool
    >>> str_to_bool("y")
    True
    >>> str_to_bool("blah", default=True)
    True
    >>> str_to_bool("n")
    False
    """
    if not isinstance(value, str):
        raise ValueError(f"Expected str, got {type(value)}")

    if value.lower() in ["1", "true", "t", "yes", "y"]:
        return True

    if value.lower() in ["0", "false", "f", "no", "n"]:
        return False

    if default is not None:
        return default

    raise ValueError("Could not convert string to bool: '{value}'")
