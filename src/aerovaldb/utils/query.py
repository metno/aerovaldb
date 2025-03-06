import sys

if sys.version_info >= (3, 12):
    from typing import override
else:
    from typing_extensions import override

from ..routes import Route


class QueryEntry:
    """Class representing information about an asset. Information
    is represented as a string representing the URI of the asset
    but additional information is available, namely type (See
    AssetType) and meta (dict containing metadata about the asset).
    """

    def __init__(self, uri: str, type: Route, meta: dict[str, str]):
        self._uri = uri
        self._type = type
        self._meta = meta

    @property
    def uri(self) -> str:
        """The URI of the asset."""
        return self._uri

    @property
    def type(self) -> Route:
        """The type of the asset this represents (see Route enum for details)."""
        return self._type

    @property
    def meta(self) -> dict[str, str]:
        """Dictionary containing metadata about the asset. Available keys should
        reflect the args passed to put for this assettype.

        :return: Dict containing metadata.
        """
        return self._meta

    @override
    def __str__(self) -> str:
        return self.uri
