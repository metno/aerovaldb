ROUTE_GLOB_STATS = "/v0/glob_stats/{project}/{experiment}/{frequency}"

ROUTE_REG_STATS = "/v0/regional_stats/{project}/{experiment}/{frequency}"

ROUTE_HEATMAP = "/v0/heatmap/{project}/{experiment}/{frequency}"

ROUTE_CONTOUR = "/v0/contour/{project}/{experiment}/{obsvar}/{model}"

ROUTE_TIMESERIES = "/v0/ts/{project}/{experiment}/{location}/{network}/{obsvar}/{layer}"

ROUTE_TIMESERIES_WEEKLY = (
    "/v0/ts_weekly/{project}/{experiment}/{location}_{network}-{obsvar}_{layer}"
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
    "/v0/scat/{project}/{experiment}/{network}/{obsvar}/{layer}/{model}/{modvar}/{time}"
)

ROUTE_PROFILES = "/v0/profiles/{project}/{experiment}/{location}/{network}/{obsvar}"

ROUTE_HEATMAP_TIMESERIES = (
    "/v0/hm_ts/{project}/{experiment}/{region}/{network}/{obsvar}/{layer}"
)

ROUTE_FORECAST = (
    "/v0/forecast/{project}/{experiment}/{region}/{network}/{obsvar}/{layer}"
)

ROUTE_GRIDDED_MAP = "/v0/gridded_map/{project}/{experiment}/{obsvar}/{model}"

ROUTE_REPORT = "/v0/report/{project}/{experiment}/{title}"
