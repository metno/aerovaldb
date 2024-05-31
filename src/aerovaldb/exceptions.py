class FileDoesNotExist(IOError):
    """
    Exception raised by jsondb in FILE_PATH mode, if the resulting file
    does not exist.
    """


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