from .aerovaldb import AerovalDB
from pathlib import Path
import logging
from aerovaldb.aerovaldb import get_method, put_method
import os
import json

logger = logging.getLogger(__name__)


class AerovalJsonFileDB(AerovalDB):
    def __init__(self, basedir: str | Path):
        self._basedir = basedir
        if isinstance(self._basedir, str):
            self._basedir = Path(self._basedir)

    def _get(self, route, route_args, *args, **kwargs):
        logger.debug(f"_get({route}, {route_args}, {args}, {kwargs})")
        print("reading from ", route.format(**route_args), ".json")
        return "dummy"

    def _put(self, obj, route, route_args, *args, **kwargs):
        logger.debug(f"_put({obj}, {route}, {route_args}, {args}, {kwargs})")
        print("writing to ", route.format(**route_args), ".json")
        return "dummyPut"

    @get_method("/glob_stats/{project}/{experiment}/{frequency}")
    def get_glob_stats(self, project: str, experiment: str, frequency: str):
        file_path = os.path.join(self._basedir, project, experiment, "hm", f'glob_stats_{frequency}.json')
        with open(file_path) as f:
            return json.load(f)