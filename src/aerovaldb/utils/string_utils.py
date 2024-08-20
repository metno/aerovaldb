import regex as re


PATH_COMPONENT_PATTERN = re.compile(r"^[^/]+$", flags=re.UNICODE)


def str_to_bool(value: str, /, strict: bool = False, default: bool = False) -> bool:
    """
    Parses a string as a boolean, supporting various values. It is intended
    mainly for parsing environment variables.

    Supports 1/0, true/false, t/f, yes/no, y/n (Case insensitive).

    :param value : The string value to be converted.
    :param strict : If true, ValueError is raised if an unrecognized value is encountered,
    otherwise default is returned.
    :param default : Returned if strict is disabled, for unrecognized values.
    :raises ValueError
        If value is not a string
    :raises ValueError
        If strict is true, and value does not match expected values.
    """
    if not isinstance(value, str):
        raise ValueError(f"Expected str, got {type(value)}")

    if value.lower() in ["1", "true", "t", "yes", "y"]:
        return True

    if value.lower() in ["0", "false", "f", "no", "n"]:
        return False

    if not strict:
        return default

    raise ValueError("Unexpected string '{value}'")


def validate_filename_component(value: str) -> None:
    """
    Checks if a file name component contains characters which should
    not be included in the file path.

    This is stricter than strictly speaking necessary but is suitable
    for aerovaldb's use case.

    :param value : The component to be validated.

    :raises ValueError :
        if value is not string or not a valid filename component
    """
    if not isinstance(value, str):
        raise ValueError(f"Expected str, got {type(value)}")

    if not PATH_COMPONENT_PATTERN.match(value):
        raise ValueError(f"'{value}' is not a valid file name component.")
