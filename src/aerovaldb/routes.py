ROUTE_GLOB_STATS = "/v0/glob_stats/{project}/{experiment}/{frequency}"

ROUTE_REG_STATS = "/v0/regional_stats/{project}/{experiment}/{frequency}"

ROUTE_HEATMAP = "/v0/heatmap/{project}/{experiment}/{frequency}"

ROUTE_CONTOUR = "/v0/contour/{project}/{experiment}/{obsvar}/{model}"
ROUTE_CONTOUR2 = "/v0/contour2/{project}/{experiment}/{obsvar}/{model}/{timestep}"

ROUTE_TIMESERIES = "/v0/ts/{project}/{experiment}/{location}/{network}/{obsvar}/{layer}"

ROUTE_TIMESERIES_WEEKLY = (
    "/v0/ts_weekly/{project}/{experiment}/{location}/{network}/{obsvar}/{layer}"
)

ROUTE_EXPERIMENTS = "/v0/experiments/{project}"

ROUTE_CONFIG = "/v0/config/{project}/{experiment}"

ROUTE_MENU = "/v0/menu/{project}/{experiment}"

ROUTE_STATISTICS = "/v0/statistics/{project}/{experiment}"

ROUTE_RANGES = "/v0/ranges/{project}/{experiment}"

ROUTE_REGIONS = "/v0/regions/{project}/{experiment}"

ROUTE_MODELS_STYLE = "/v0/model_style/{project}"

ROUTE_MAP = "/v0/map/{project}/{experiment}/{network}/{obsvar}/{layer}/{model}/{modvar}"

ROUTE_SCATTER = (
    "/v0/scat/{project}/{experiment}/{network}/{obsvar}/{layer}/{model}/{modvar}"
)

ROUTE_PROFILES = "/v0/profiles/{project}/{experiment}/{location}/{network}/{obsvar}"

ROUTE_HEATMAP_TIMESERIES = "/v0/hm_ts/{project}/{experiment}"

ROUTE_FORECAST = (
    "/v0/forecast/{project}/{experiment}/{region}/{network}/{obsvar}/{layer}"
)

ROUTE_GRIDDED_MAP = "/v0/gridded_map/{project}/{experiment}/{obsvar}/{model}"

ROUTE_REPORT = "/v0/report/{project}/{experiment}/{title}"

ROUTE_REPORT_IMAGE = "/v0/report-image/{project}/{experiment}/{path}"

ROUTE_MAP_OVERLAY = "/v0/map-overlay/{project}/{experiment}/{source}/{variable}/{date}"

ALL_ROUTES = [
    ROUTE_GLOB_STATS,
    ROUTE_REG_STATS,
    ROUTE_HEATMAP,
    ROUTE_CONTOUR,
    ROUTE_CONTOUR2,
    ROUTE_TIMESERIES,
    ROUTE_TIMESERIES_WEEKLY,
    ROUTE_EXPERIMENTS,
    ROUTE_CONFIG,
    ROUTE_MENU,
    ROUTE_STATISTICS,
    ROUTE_RANGES,
    ROUTE_REGIONS,
    ROUTE_MODELS_STYLE,
    ROUTE_MAP,
    ROUTE_SCATTER,
    ROUTE_PROFILES,
    ROUTE_HEATMAP_TIMESERIES,
    ROUTE_FORECAST,
    ROUTE_GRIDDED_MAP,
    ROUTE_REPORT,
    ROUTE_REPORT_IMAGE,
    ROUTE_MAP_OVERLAY,
]
