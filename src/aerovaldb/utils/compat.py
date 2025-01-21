import sys

if sys.version_info >= (3, 12):
    from typing import override

    override = override
else:

    def override(method, /):
        """
        A wrapper for the typing.override wrapper, that ensures it does nothing on unsupported
        python versions (<3.12).

        Since this wrapper is for the benefit of typecheckers (eg. mypy), we don't want it
        to do anything on unsupported versions.
        https://docs.python.org/3/library/typing.html#typing.override
        """
        return method
