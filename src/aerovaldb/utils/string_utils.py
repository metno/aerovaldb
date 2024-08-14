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
