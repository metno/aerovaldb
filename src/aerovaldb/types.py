from enum import Enum, auto

from .routes import *


class AssetType(Enum):
    GLOB_STATS = ROUTE_GLOB_STATS
    REGIONAL_STATS = ROUTE_REG_STATS
    HEATMAP = ROUTE_HEATMAP
    CONTOUR = ROUTE_CONTOUR
    CONTOUR_TIMESPLIT = ROUTE_CONTOUR2
    TIMESERIES = ROUTE_TIMESERIES
    TIMESERIES_WEEKLY = ROUTE_TIMESERIES_WEEKLY
    EXPERIMENTS = ROUTE_EXPERIMENTS
    CONFIG = ROUTE_CONFIG
    MENU = ROUTE_MENU
    STATISTICS = ROUTE_STATISTICS
    RANGES = ROUTE_RANGES
    REGIONS = ROUTE_REGIONS
    MODELS_STYLE = ROUTE_MODELS_STYLE
    MAP = ROUTE_MAP
    SCATTER = ROUTE_SCATTER
    PROFILES = ROUTE_PROFILES
    HEATMAP_TIMESERIES = ROUTE_HEATMAP_TIMESERIES
    FORECAST = ROUTE_FORECAST
    GRIDDED_MAP = ROUTE_GRIDDED_MAP
    REPORT = ROUTE_REPORT
    REPORT_IMAGE = ROUTE_REPORT_IMAGE
    MAP_OVERLAY = ROUTE_MAP_OVERLAY


class AccessType(Enum):
    """Enumeration of access types. Specifies how data will be read
    and returned.

    * JSON_STR: Result will be returned as an unparsed json string.

    * FILE_PATH: Result will be returned as the file path to the file
    containing the data.

    * OBJ: The json will be parsed and returned as a python object.

    * URI: A string which is a unique identifier of this asset between
    implementations of Aerovaldb. Can be used with :meth:`aerovaldb.AerovalDB.get_by_uuid`
    and :meth:`aerovaldb.AerovalDB.put_by_uuid` to read or write respectively.

    * MTIME: The timestamp for last modification for the resource will be
    returned (as datetime.datetime).

    * CTIME: The creation timestamp for the resource will be returned (as
    datetime.datetime)
    """

    JSON_STR = auto()
    FILE_PATH = auto()
    OBJ = auto()
    URI = auto()
    BLOB = auto()
    MTIME = auto()
    CTIME = auto()
