from ..types import AssetType


class QueryEntry(str):
    """Class representing information about an asset. Information
    is represented as a string representing the URI of the asset
    but additional information is available, namely type (See
    AssetType) and meta (dict containing metadata about the asset).
    """

    def __new__(cls, value: str, type: AssetType, meta: dict[str, str]):
        if not isinstance(value, str):
            raise TypeError(f"Provided value must be str, got {type(value)}.")

        instance = super().__new__(cls, value)
        return instance

    def __init__(self, value: str, type: AssetType, meta: dict[str, str]):
        self._type = type
        self._meta = meta

    @property
    def type(self) -> AssetType:
        """The type of the asset this represents (see AssetType for details)."""
        return self._type

    @property
    def meta(self) -> dict[str, str]:
        """Dictionary containing metadata about the asset. Available keys should
        reflect the args passed to put for this assettype.

        :return: Dict containing metadata.
        """
        return self._meta
