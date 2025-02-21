from ..types import AssetType


class QueryEntry(str):
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
        return self._type

    @property
    def meta(self) -> dict[str, str]:
        return self._meta
