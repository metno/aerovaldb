class UnusedArguments(ValueError):
    """
    Raised by jsondb if args or kwargs remain after matching (which
    is likely a mistake by the caller).
    """


class TemplateNotFound(KeyError):
    """
    Raised by jsondb if no matching filepath template was found for
    the requested version of data.
    """


class UnsupportedOperation(NotImplementedError):
    """
    Raised if some operation could not be provided for some reason.
    """
