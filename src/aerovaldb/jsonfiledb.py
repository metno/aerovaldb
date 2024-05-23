from .aerovaldb import AerovalDB
from pathlib import Path
import logging
from aerovaldb.aerovaldb import get_method, put_method
import os
import json
import orjson
from enum import Enum

AccessType = Enum("AccessType", ["JSON_STR", "FILE_PATH", "OBJ"])

logger = logging.getLogger(__name__)


class AerovalJsonFileDB(AerovalDB):
    def __init__(self, basedir: str | Path):
        self._basedir = basedir
        if isinstance(self._basedir, str):
            self._basedir = Path(self._basedir)

        self.PATH_LOOKUP = {
            "/v0/glob_stats/{project}/{experiment}/{frequency}": "./{project}/{experiment}/hm/glob_stats_{frequency}.json",
            "/v0/contour/{project}/{experiment}": "./{project}/{experiment}/contour/modvar_model.geojson",
            "/v0/ts/{project}/{experiment}/{region}/{network}/{obsvar}/{layer}": "./{project}/{experiment}/ts/{region}_{network}-{obsvar}_{layer}.json",
            "/v0/experiments/{project}": "./{project}/experiments.json",
            "/v0/config/{project}/{experiment}": "./{project}/{experiment}/cfg_{project}_{experiment}.json",
            "/v0/menu/{project}/{experiment}": "./{project}/{experiment}/menu.json",
            "/v0/statistics/{project}/{experiment}": "./{project}/{experiment}/statistics.json",
            "/v0/ranges/{project}/{experiment}": "./{project}/{experiment}/ranges.json",
            "/v0/regions/{project}/{experiment}": "./{project}/{experiment}/regions.json",
        }

    def _normalize_access_type(
        self, access_type: AccessType | str | None, default: AccessType = AccessType.OBJ
    ) -> AccessType:
        """Normalizes the access_type to an instance of AccessType enum.

        :param access_type: AccessType instance or string convertible to AccessType
        :param default: The type to return if access_type is None. Defaults to AccessType.OBJ
        :raises ValueError: If str access_type can't be converted to AccessType.
        :raises ValueError: If access_type is not str or AccessType
        :return: The normalized AccessType.
        """
        if isinstance(access_type, AccessType):
            return access_type
        if isinstance(access_type, str):
            try:
                return AccessType[access_type]
            except:
                raise ValueError(
                    f"String '{access_type}' can not be converted to AccessType."
                )
        if access_type is None:
            return default

        raise ValueError(
            f"Access_type, {access_type}, could not be normalized. This is probably due to input that is not a str or AccessType instance."
        )

    def _get_file_path_from_route(self, route, route_args):
        file_path_template = self.PATH_LOOKUP.get(route, None)
        if file_path_template is None:
            raise KeyError(f"No file path template found for route {route}.")

        relative_path = file_path_template.format(**route_args)

        return Path(os.path.join(self._basedir, relative_path)).resolve()

    def _get(self, route, route_args, *args, **kwargs):
        access_type = self._normalize_access_type(kwargs.get("access_type", None))

        file_path = self._get_file_path_from_route(route, route_args)
        logger.debug(
            f"Mapped route {route} / { route_args} to file {file_path} with type {access_type}."
        )

        if access_type == AccessType.FILE_PATH:
            return str(file_path)

        if access_type == AccessType.JSON_STR:
            with open(file_path, "rb") as f:
                json = str(f.read())

            return json

        with open(file_path, "rb") as f:
            raw = f.read()

        return orjson.loads(raw)

    def _put(self, obj, route, route_args, *args, **kwargs):
        file_path = self._get_file_path_from_route(route, route_args)
        logger.debug(f"Mapped route {route} / { route_args} to file {file_path}.")

        if not os.path.exists(os.path.dirname(file_path)):
            os.makedirs(os.path.dirname(file_path))

        json = orjson.dumps(obj)
        with open(file_path, "wb") as f:
            f.write(json)
