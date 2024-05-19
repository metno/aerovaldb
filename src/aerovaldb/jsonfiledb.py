from .aerovaldb import AerovalDB
import logging

logger = logging.getLogger(__name__)


class AerovalJsonFileDB(AerovalDB):
    def __init__(self, basedir):
        self._basedir = basedir

    def _get(self, route, route_args, *args, **kwargs):
        logger.debug(f"_get({route}, {route_args}, {args}, {kwargs})")
        print("reading from ", route.format(**route_args), ".json")
        return "dummy"

    def _put(self, obj, route, route_args, *args, **kwargs):
        logger.debug(f"_put({obj}, {route}, {route_args}, {args}, {kwargs})")
        print("writing to ", route.format(**route_args), ".json")
        return "dummyPut"
