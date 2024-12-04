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


def validate_filename_component(value: str) -> None:
    """
    Checks if a file name component contains characters which should
    not be included in the file path.

    :param value : The component to be validated.

    :raises ValueError :
        if value is not string or not a valid filename component

    >>> from aerovaldb.utils.string_utils import validate_filename_component
    >>> validate_filename_component("Hello world")
    >>> validate_filename_component(5)
    Traceback (most recent call last):
    ...
    ValueError: Expected str, got <class 'int'>
    >>> validate_filename_component("/")
    Traceback (most recent call last):
    ...
    ValueError: '/' is not a valid file name component.
    """
    if not isinstance(value, str):
        raise ValueError(f"Expected str, got {type(value)}")

    if not PATH_COMPONENT_PATTERN.match(value):
        raise ValueError(f"'{value}' is not a valid file name component.")
