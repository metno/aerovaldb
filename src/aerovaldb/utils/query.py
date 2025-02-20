from typing import NamedTuple

from ..types import AssetType
from .uri import parse_uri


class QueryEntry(NamedTuple):
    uri: str
    type: AssetType
    args: dict[str, str]


class QueryResult:
    def __init__(self, result: list[str] | list[QueryEntry]):
        if not isinstance(result, list):
            raise TypeError(f"Expected list, got {type(result)}.")

        if all(isinstance(i, str) for i in result):
            res: list[QueryEntry] = []
            for r in result:
                assert isinstance(r, str)
                route, route_args, kwargs = parse_uri(r)
                res.append(QueryEntry(r, AssetType(route), route_args | kwargs))

            self._result = res
        elif all(isinstance(i, QueryEntry) for i in result):
            self._result = result  # type: ignore
        else:
            raise TypeError(f"All result entries must be of type str or QueryEntry.")

    def __getitem__(self, index: int):
        return self._result[index].uri

    def __len__(self):
        return len(self._result)

    def get_details(self, uri: str) -> tuple[str, dict[str, str]]:
        for e in self._result:
            if e.uri == uri:
                return e.type, e.args  # type: ignore
        else:
            raise KeyError
