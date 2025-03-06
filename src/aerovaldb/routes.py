from enum import Enum

_ROUTE_GLOB_STATS = "/v0/glob_stats/{project}/{experiment}/{frequency}"

_ROUTE_REG_STATS = "/v0/regional_stats/{project}/{experiment}/{frequency}"

_ROUTE_HEATMAP = "/v0/heatmap/{project}/{experiment}/{frequency}"

_ROUTE_CONTOUR = "/v0/contour/{project}/{experiment}/{obsvar}/{model}"
_ROUTE_CONTOUR_TIMESPLIT = (
    "/v0/contour2/{project}/{experiment}/{obsvar}/{model}/{timestep}"
)

_ROUTE_TIMESERIES = (
    "/v0/ts/{project}/{experiment}/{location}/{network}/{obsvar}/{layer}"
)

_ROUTE_TIMESERIES_WEEKLY = (
    "/v0/ts_weekly/{project}/{experiment}/{location}/{network}/{obsvar}/{layer}"
)

_ROUTE_EXPERIMENTS = "/v0/experiments/{project}"

_ROUTE_CONFIG = "/v0/config/{project}/{experiment}"

_ROUTE_MENU = "/v0/menu/{project}/{experiment}"

_ROUTE_STATISTICS = "/v0/statistics/{project}/{experiment}"

_ROUTE_RANGES = "/v0/ranges/{project}/{experiment}"

_ROUTE_REGIONS = "/v0/regions/{project}/{experiment}"

_ROUTE_MODELS_STYLE = "/v0/model_style/{project}"

_ROUTE_MAP = (
    "/v0/map/{project}/{experiment}/{network}/{obsvar}/{layer}/{model}/{modvar}"
)

_ROUTE_SCATTER = (
    "/v0/scat/{project}/{experiment}/{network}/{obsvar}/{layer}/{model}/{modvar}"
)

_ROUTE_PROFILES = "/v0/profiles/{project}/{experiment}/{location}/{network}/{obsvar}"

_ROUTE_HEATMAP_TIMESERIES = "/v0/hm_ts/{project}/{experiment}"

_ROUTE_FORECAST = (
    "/v0/forecast/{project}/{experiment}/{region}/{network}/{obsvar}/{layer}"
)

_ROUTE_GRIDDED_MAP = "/v0/gridded_map/{project}/{experiment}/{obsvar}/{model}"

_ROUTE_REPORT = "/v0/report/{project}/{experiment}/{title}"

_ROUTE_REPORT_IMAGE = "/v0/report-image/{project}/{experiment}/{path}"

_ROUTE_MAP_OVERLAY = "/v0/map-overlay/{project}/{experiment}/{source}/{variable}/{date}"


class Route(Enum):
    GLOB_STATS = _ROUTE_GLOB_STATS
    REGIONAL_STATS = _ROUTE_REG_STATS
    HEATMAP = _ROUTE_HEATMAP
    CONTOUR = _ROUTE_CONTOUR
    CONTOUR_TIMESPLIT = _ROUTE_CONTOUR_TIMESPLIT
    TIMESERIES = _ROUTE_TIMESERIES
    TIMESERIES_WEEKLY = _ROUTE_TIMESERIES_WEEKLY
    EXPERIMENTS = _ROUTE_EXPERIMENTS
    CONFIG = _ROUTE_CONFIG
    MENU = _ROUTE_MENU
    STATISTICS = _ROUTE_STATISTICS
    RANGES = _ROUTE_RANGES
    REGIONS = _ROUTE_REGIONS
    MODELS_STYLE = _ROUTE_MODELS_STYLE
    MAP = _ROUTE_MAP
    SCATTER = _ROUTE_SCATTER
    PROFILES = _ROUTE_PROFILES
    HEATMAP_TIMESERIES = _ROUTE_HEATMAP_TIMESERIES
    FORECAST = _ROUTE_FORECAST
    GRIDDED_MAP = _ROUTE_GRIDDED_MAP
    REPORT = _ROUTE_REPORT
    REPORT_IMAGE = _ROUTE_REPORT_IMAGE
    MAP_OVERLAY = _ROUTE_MAP_OVERLAY
