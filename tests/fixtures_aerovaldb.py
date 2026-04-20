import pytest

import aerovaldb


@pytest.fixture
def tmpdb(tmp_path, dbtype: str) -> aerovaldb.AerovalDB:
    """Fixture encapsulating logic for each tested database connection to create
    a fresh, temporary database and connect to it."""
    if dbtype == "json_files":
        return aerovaldb.open(f"json_files:{str(tmp_path)}")
    elif dbtype == "sqlitedb":
        return aerovaldb.open(":memory:")

    assert False


TESTDB_PARAMETRIZATION = pytest.mark.parametrize(
    # This is a parametrization which returns the correct resource string to access
    # the prebuilt test database for each database connector.
    "testdb",
    (
        pytest.param(
            "json_files:./tests/test-db/json",
        ),
        pytest.param(
            "sqlitedb:./tests/test-db/sqlite/test.sqlite",
        ),
    ),
)

IMPLEMENTATION_PARAMETRIZATION = pytest.mark.parametrize(
    "dbtype", (pytest.param("json_files"), pytest.param("sqlitedb"))
)
GET_PARAMETRIZATION = pytest.mark.parametrize(
    "fun,args,kwargs,expected",
    (
        (
            "get_glob_stats",
            ["project", "experiment", "frequency"],
            None,
            "./project/experiment/hm/",
        ),
        (
            "get_regional_stats",
            ["project", "experiment", "frequency", "network", "variable", "layer"],
            None,
            "./project/experiment/hm/regional_stats",
        ),
        pytest.param(
            "get_heatmap",
            ["project", "experiment", "frequency", "region", "time"],
            None,
            "./project/experiment/hm/regional_stats",
            marks=pytest.mark.xfail(reason="missing test file in json testdb"),
        ),
        (
            "get_contour",
            ["project", "experiment", "modvar", "model"],
            {"timestep": "timestep"},
            "748956457892",
        ),
        (
            "get_contour",
            ["project", "experiment", "modvar", "model"],
            {"timestep": "timestep2"},
            "2758924570298570",
        ),
        (
            "get_timeseries",
            ["project", "experiment", "location", "network", "obsvar", "layer"],
            None,
            "./project/experiment/ts/",
        ),
        (
            "get_timeseries_weekly",
            ["project", "experiment", "location", "network", "obsvar", "layer"],
            None,
            "./project/experiment/ts/dirunal/",
        ),
        ("get_config", ["project", "experiment"], None, "./project/experiment/"),
        ("get_menu", ["project", "experiment"], None, "./project/experiment/"),
        ("get_statistics", ["project", "experiment"], None, "./project/experiment/"),
        ("get_ranges", ["project", "experiment"], None, "./project/experiment/"),
        ("get_regions", ["project", "experiment"], None, "./project/experiment/"),
        ("get_models_style", ["project"], None, "./project/"),
        ("get_experiments", ["project"], None, "./project/"),
        (
            "get_models_style",
            ["project"],
            {"experiment": "experiment"},
            "./project/experiment/",
        ),
        (
            "get_map",
            [
                "project",
                "experiment-old",
                "network",
                "obsvar",
                "layer",
                "model",
                "modvar",
                "time",
            ],
            None,
            "./project/experiment/map/",
        ),
        (
            "get_map",
            [
                "project",
                "experiment",
                "network",
                "obsvar",
                "layer",
                "model",
                "modvar",
                "time",
            ],
            None,
            "./project/experiment/map/with_time",
        ),
        (
            "get_scatter",
            [
                "project",
                "experiment",
                "network",
                "obsvar",
                "layer",
                "model",
                "modvar",
                "time",
            ],
            None,
            "./project/experiment/scat/time",
        ),
        (
            "get_scatter",
            [
                "project",
                "experiment-old",
                "network",
                "obsvar",
                "layer",
                "model",
                "modvar",
                "test",
            ],
            None,
            "./project/experiment/scat/",
        ),
        (
            "get_profiles",
            ["project", "experiment", "region", "network", "obsvar"],
            None,
            "./project/experiment/profiles/",
        ),
        (
            "get_heatmap_timeseries",
            ["project", "experiment", "region", "network", "obsvar", "layer"],
            None,
            "./project/experiment/hm/ts/region-network-obsvar-layer",
        ),
        (
            "get_heatmap_timeseries",
            ["project", "experiment-old", "region", "network", "obsvar", "layer"],
            None,
            "project/experiment/hm/ts/stats_ts.json",
        ),
        # TODO: Missing test case for heatmap_ts with the middle version format.
        (
            "get_forecast",
            ["project", "experiment", "region", "network", "obsvar", "layer"],
            None,
            "./project/experiment/forecast/",
        ),
        (
            "get_fairmode",
            [
                "project",
                "experiment",
                "region",
                "network",
                "obsvar",
                "layer",
                "model",
                "time",
            ],
            None,
            "./project/experiment/fairmode/",
        ),
        (
            "get_gridded_map",
            ["project", "experiment", "obsvar", "model"],
            None,
            "./project/experiment/contour/",
        ),
        (
            "get_report",
            ["project", "experiment", "title"],
            None,
            "./reports/project/experiment/",
        ),
    ),
)

PUT_PARAMETRIZATION = pytest.mark.parametrize(
    "fun,args,kwargs",
    (
        ("glob_stats", ["project", "experiment", "frequency"], None),
        (
            "timeseries",
            ["project", "experiment", "location", "network", "obsvar", "layer"],
            None,
        ),
        (
            "contour",
            ["project", "experiment", "obsvar", "model"],
            {"timestep": "timestep"},
        ),
        (
            "timeseries_weekly",
            ["project", "experiment", "location", "network", "obsvar", "layer"],
            None,
        ),
        ("config", ["project", "experiment"], None),
        ("menu", ["project", "experiment"], None),
        ("statistics", ["project", "experiment"], None),
        ("ranges", ["project", "experiment"], None),
        ("regions", ["project", "experiment"], None),
        ("models_style", ["project"], None),
        ("models_style", ["project"], {"experiment": "experiment"}),
        (
            "map",
            [
                "project",
                "experiment",
                "network",
                "obsvar",
                "layer",
                "model",
                "modvar",
                "time",
            ],
            None,
        ),
        (
            "map",
            [
                "project",
                "experiment",
                "network",
                "obsvar",
                "layer",
                "model",
                "modvar",
                "time",
            ],
            None,
        ),
        (
            "scatter",
            [
                "project",
                "experiment",
                "network",
                "obsvar",
                "layer",
                "model",
                "modvar",
                "time",
            ],
            None,
        ),
        (
            "scatter",
            [
                "project",
                "experiment",
                "network",
                "obsvar",
                "layer",
                "model",
                "modvar",
                "time",
            ],
            None,
        ),
        ("profiles", ["project", "experiment", "station", "network", "obsvar"], None),
        (
            "heatmap_timeseries",
            ["project", "experiment", "region", "network", "obsvar", "layer"],
            None,
        ),
        (
            "forecast",
            ["project", "experiment", "station", "network", "obsvar", "layer"],
            None,
        ),
        (
            "fairmode",
            [
                "project",
                "experiment",
                "station",
                "network",
                "obsvar",
                "layer",
                "model",
                "time",
            ],
            None,
        ),
        ("gridded_map", ["project", "experiment", "obsvar", "model"], None),
        ("report", ["project", "experiment", "title"], None),
    ),
)
