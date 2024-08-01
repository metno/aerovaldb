# This test file applies tests to each implementation of database connector
# through the interface defined in src/aerovaldb/aerovaldb.py
# In addition each implementation has implementation specific tests in its
# respective test_*.py file
# - json_files: tests/jsondb/test_jsonfiledb.py
# - sqlitedb:   tests/sqlitedb/test_sqlitedb.py

import aerovaldb
import pytest
import random


@pytest.fixture
def tmpdb(tmp_path, dbtype: str) -> aerovaldb.AerovalDB:
    """Fixture encapsulating logic for each tested database connection to create
    a temporary database and connect to it."""
    if dbtype == "json_files":
        return aerovaldb.open(f"json_files:{str(tmp_path)}")
    elif dbtype == "sqlitedb":
        return aerovaldb.open(":memory:")

    assert False


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
            None,
            "./project/experiment/contour/",
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
        ("contour", ["project", "experiment", "obsvar", "model"], None),
        (
            "timeseries",
            ["project", "experiment", "location", "network", "obsvar", "layer"],
            None,
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
        ("gridded_map", ["project", "experiment", "obsvar", "model"], None),
        ("report", ["project", "experiment", "title"], None),
    ),
)


@pytest.mark.asyncio
@pytest.mark.parametrize("resource", (("json_files:./tests/test-db/json",)))
@GET_PARAMETRIZATION
async def test_getter(resource: str, fun: str, args: list, kwargs: dict, expected):
    """
    This test tests that data is read as expected from a static, fixed database.
    """
    with aerovaldb.open(resource, use_async=True) as db:
        f = getattr(db, fun)

        if kwargs is not None:
            data = await f(*args, **kwargs)
        else:
            data = await f(*args)

        assert data["path"] == expected


@pytest.mark.parametrize("resource", (("json_files:./tests/test-db/json",)))
@GET_PARAMETRIZATION
def test_getter_sync(resource: str, fun: str, args: list, kwargs: dict, expected):
    with aerovaldb.open(resource, use_async=False) as db:
        f = getattr(db, fun)

        if kwargs is not None:
            data = f(*args, **kwargs)
        else:
            data = f(*args)

        assert data["path"] == expected


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "dbtype", (pytest.param("json_files"), pytest.param("sqlitedb"))
)
@PUT_PARAMETRIZATION
async def test_setters(dbtype: str, fun: str, args: list, kwargs: dict, tmpdb):
    """
    This test tests that you read back the expected data, once you have written
    to a fresh db, assuming the same arguments.
    """
    with tmpdb as db:
        get = getattr(db, f"get_{fun}")
        put = getattr(db, f"put_{fun}")

        expected = fun + str(random.randint(0, 100000))
        if kwargs is not None:
            await put({"data": expected}, *args, **kwargs)

            data = await get(*args, **kwargs)
        else:
            await put({"data": expected}, *args)

            data = await get(*args)

        assert data["data"] == expected


@pytest.mark.parametrize(
    "dbtype", (pytest.param("json_files"), pytest.param("sqlitedb"))
)
@PUT_PARAMETRIZATION
def test_setters_sync(fun: str, args: list, kwargs: dict, tmpdb):
    """
    This test tests that you read back the expected data, once you have written
    to a fresh db, assuming the same arguments.
    """
    with tmpdb as db:
        get = getattr(db, f"get_{fun}")
        put = getattr(db, f"put_{fun}")

        expected = fun + str(random.randint(0, 100000))
        if kwargs is not None:
            put({"data": expected}, *args, **kwargs)

            data = get(*args, **kwargs)
        else:
            put({"data": expected}, *args)

            data = get(*args)

        assert data["data"] == expected


@pytest.mark.parametrize(
    "dbtype",
    (
        pytest.param(
            "json_files",
        ),
        pytest.param(
            "sqlitedb",
        ),
    ),
)
def test_write_and_read_of_nan(tmpdb):
    with tmpdb as db:
        data = dict(value=float("nan"))

        db.put_by_uri(data, "/v0/experiments/project")

        read = db.get_by_uri("/v0/experiments/project")

        # See Additional Notes on #59
        # https://github.com/metno/aerovaldb/pull/59
        assert read["value"] is None