class FileDoesNotExist(IOError):
    """
    Exception raised by jsondb in FILE_PATH mode, if the resulting file
    does not exist.
    """
