import regex as re


PATH_COMPONENT_PATTERN = re.compile(r"^[^/]+$", flags=re.UNICODE)


def str_to_bool(value: str, /, default: bool | None = None) -> bool:
    """
    Parses a string as a boolean, supporting various values. It is intended
    mainly for parsing environment variables.

    Supports 1/0, true/false, t/f, yes/no, y/n (Case insensitive).

    :param value : The string value to be converted.
    :param default : If None, a string value not explicitly matching one of the
        above values for true/false will raise a ValueError. Otherwise this default
        will be returned instead.

    :raises ValueError
        If value is not a string
    :raises ValueError
        If default is None, and value does not match one of the above templates.
    """
    if not isinstance(value, str):
        raise ValueError(f"Expected str, got {type(value)}")

    if value.lower() in ["1", "true", "t", "yes", "y"]:
        return True

    if value.lower() in ["0", "false", "f", "no", "n"]:
        return False

    if default is not None:
        return default

    raise ValueError("Could not convert string to float: '{value}'")


def validate_filename_component(value: str) -> None:
    """
    Checks if a file name component contains characters which should
    not be included in the file path.

    :param value : The component to be validated.

    :raises ValueError :
        if value is not string or not a valid filename component
    """
    if not isinstance(value, str):
        raise ValueError(f"Expected str, got {type(value)}")

    if not PATH_COMPONENT_PATTERN.match(value):
        raise ValueError(f"'{value}' is not a valid file name component.")
