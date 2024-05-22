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
            "/glob_stats/{project}/{experiment}/{frequency}": "./{project}/{experiment}/hm/glob_stats_{frequency}.json"
        }

    def _get_file_path_from_route(self, route, route_args):
        file_path_template = self.PATH_LOOKUP.get(route, None)
        if file_path_template is None:
            raise KeyError(f"No file path template found for route {route}.")

        relative_path = file_path_template.format(**route_args)

        return Path(os.path.join(self._basedir, relative_path)).resolve()

    def _get(self, route, route_args, *args, **kwargs):
        access_type = kwargs.get("type", AccessType.JSON_STR)

        file_path = self._get_file_path_from_route(route, route_args)

        if access_type == AccessType.FILE_PATH:
            return str(file_path)

        logger.debug(f"Mapped route {route} / { route_args} to file {file_path}.")

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
