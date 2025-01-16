# This test file applies tests to each implementation of database connector
# through the interface defined in src/aerovaldb/aerovaldb.py
# In addition each implementation has implementation specific tests in its
# respective test_*.py file
# - json_files: tests/jsondb/test_jsonfiledb.py
# - sqlitedb:   tests/sqlitedb/test_sqlitedb.py

import datetime
import pathlib
import random

import filetype
import pytest
import simplejson  # type: ignore

import aerovaldb
import aerovaldb.jsondb
from aerovaldb.utils.copy import copy_db_contents


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
        ("gridded_map", ["project", "experiment", "obsvar", "model"], None),
        ("report", ["project", "experiment", "title"], None),
    ),
)


@pytest.mark.asyncio
@TESTDB_PARAMETRIZATION
@GET_PARAMETRIZATION
async def test_getter(testdb: str, fun: str, args: list, kwargs: dict, expected):
    """
    This test tests that data is read as expected from a static, fixed database.
    """
    with aerovaldb.open(testdb, use_async=True) as db:
        f = getattr(db, fun)

        if kwargs is not None:
            data = await f(*args, **kwargs)
        else:
            data = await f(*args)

        assert data["path"] == expected


@TESTDB_PARAMETRIZATION
@GET_PARAMETRIZATION
def test_getter_sync(testdb: str, fun: str, args: list, kwargs: dict, expected):
    with aerovaldb.open(testdb, use_async=False) as db:
        f = getattr(db, fun)

        if kwargs is not None:
            data = f(*args, **kwargs)
        else:
            data = f(*args)

        assert data["path"] == expected


@TESTDB_PARAMETRIZATION
@GET_PARAMETRIZATION
def test_getter_json_str(testdb: str, fun: str, args: list, kwargs: dict, expected):
    with aerovaldb.open(testdb, use_async=False) as db:
        f = getattr(db, fun)

        if kwargs is not None:
            data = f(*args, access_type=aerovaldb.AccessType.JSON_STR, **kwargs)
        else:
            data = f(*args, access_type=aerovaldb.AccessType.JSON_STR)

        data = simplejson.loads(data)
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
    "dbtype", (pytest.param("json_files"), pytest.param("sqlitedb"))
)
@PUT_PARAMETRIZATION
def test_setters_json_str(fun: str, args: list, kwargs: dict, tmpdb):
    with tmpdb as db:
        get = getattr(db, f"get_{fun}")
        put = getattr(db, f"put_{fun}")

        expected = fun + str(random.randint(0, 100000))
        if kwargs is not None:
            put(aerovaldb.utils.json_dumps_wrapper({"data": expected}), *args, **kwargs)

            data = get(*args, **kwargs)
        else:
            put(aerovaldb.utils.json_dumps_wrapper({"data": expected}), *args)

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


@pytest.mark.asyncio
@TESTDB_PARAMETRIZATION
async def test_file_does_not_exist(testdb):
    with aerovaldb.open(testdb) as db:
        with pytest.raises(FileNotFoundError):
            await db.get_config(
                "non-existent-project",
                "experiment",
            )


@TESTDB_PARAMETRIZATION
def test_getter_with_default(testdb):
    with aerovaldb.open(testdb) as db:
        data = db.get_by_uri(
            "/v0/experiments/non-existent-project", default={"data": "test"}
        )

        assert data["data"] == "test"


@TESTDB_PARAMETRIZATION
def test_getter_with_default_error(testdb):
    with aerovaldb.open(testdb) as db:
        with pytest.raises(simplejson.JSONDecodeError):
            db.get_by_uri(
                "/v0/report/project/experiment/invalid-json",
                default={"data": "data"},
            )


@TESTDB_PARAMETRIZATION
def test_version1(testdb):
    """ """
    with aerovaldb.open(testdb) as db:
        assert str(db._get_version("project", "experiment")) == "0.13.5"


@TESTDB_PARAMETRIZATION
def test_version2(testdb):
    """ """
    with aerovaldb.open(testdb) as db:
        assert str(db._get_version("project", "experiment-old")) == "0.0.5"


@TESTDB_PARAMETRIZATION
def test_list_glob_stats(testdb):
    with aerovaldb.open(testdb) as db:
        glob_stats = db.list_glob_stats("project", "experiment")

        assert len(glob_stats) == 1


@TESTDB_PARAMETRIZATION
def test_list_all(testdb):
    with aerovaldb.open(testdb) as db:
        assert len(db.list_all()) == 49


@TESTDB_PARAMETRIZATION
def test_list_timeseries(testdb):
    with aerovaldb.open(testdb) as db:
        timeseries = db.list_timeseries("project", "experiment")

        assert len(list(timeseries)) == 1


@pytest.mark.parametrize(
    "dbtype", (pytest.param("json_files"), pytest.param("sqlitedb"))
)
def test_rm_experiment_data(tmpdb):
    with aerovaldb.open("json_files:./tests/test-db/json") as db:
        copy_db_contents(db, tmpdb)
        assert len(list(db.list_all())) == len(list(tmpdb.list_all()))

        tmpdb.rm_experiment_data("project", "experiment")

        assert len(list(tmpdb.list_all())) == 30


@TESTDB_PARAMETRIZATION
@pytest.mark.parametrize(
    "sub_path",
    (
        pytest.param("img/pixel.avif"),
        pytest.param("img/pixel.gif"),
        pytest.param("img/pixel.jpeg"),
        pytest.param("img/pixel.jpg"),
        pytest.param("img/pixel.png"),
        pytest.param("img/pixel.webp"),
    ),
)
def test_get_report_image(testdb, sub_path: str):
    with aerovaldb.open(testdb) as db:
        blob = db.get_report_image(
            "project",
            "experiment",
            sub_path,
            access_type=aerovaldb.AccessType.BLOB,
        )
        assert isinstance(blob, bytes)
        assert len(blob) > 0


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
def test_put_report_image(tmpdb):
    with open("tests/test-db/json/reports/project/experiment/img/pixel.png", "rb") as f:
        data = f.read()

    with tmpdb as db:
        db.put_report_image(data, "project", "experiment", "pixel.png")

        blob = db.get_report_image("project", "experiment", "pixel.png")

    assert isinstance(blob, bytes)
    assert len(blob) > 0


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
def test_serialize_set(tmpdb):
    with tmpdb as db:
        db.put_config({"set": {"a", "b", "c"}}, "test", "test")


@TESTDB_PARAMETRIZATION
def test_get_times(testdb):
    with aerovaldb.open(testdb) as db:
        for uri in db.list_all():
            mtime = db.get_by_uri(uri, access_type="MTIME")
            ctime = db.get_by_uri(uri, access_type="CTIME")

            assert isinstance(mtime, datetime.datetime)
            assert isinstance(ctime, datetime.datetime)
            assert mtime.year >= 2024 and mtime < datetime.datetime.now()
            assert ctime.year >= 2024 and ctime < datetime.datetime.now()


@TESTDB_PARAMETRIZATION
def test_get_experiment_mtime(testdb):
    with aerovaldb.open(testdb) as db:
        for exp in ["experiment", "experiment-old"]:
            mtime = db.get_experiment_mtime("project", exp)

            assert isinstance(mtime, datetime.datetime)
            assert mtime.year >= 2024 and mtime < datetime.datetime.now()


TEST_IMAGES = {
    ".webp": pathlib.Path("tests/test_img/test.webp"),
    ".png": pathlib.Path("tests/test_img/test.png"),
}


@TESTDB_PARAMETRIZATION
def test_get_map_overlay(testdb):
    with aerovaldb.open(testdb) as db:
        data: bytes = db.get_map_overlay(
            "project",
            "experiment",
            "source",
            "variable",
            "date",
            access_type=aerovaldb.AccessType.BLOB,
        )

        assert filetype.guess_extension(data) == "png"


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
@pytest.mark.parametrize(
    "expected_extension",
    (
        pytest.param(".png", id="png"),
        pytest.param(".webp", id="webp"),
    ),
)
def test_put_map_overlay(tmpdb, expected_extension: str):
    with open(TEST_IMAGES[expected_extension], "rb") as f:
        input_data = f.read()

    with tmpdb as db:
        db.put_map_overlay(
            input_data,
            "project",
            "experiment",
            "source",
            "variable",
            "date",
        )

        data: bytes = db.get_map_overlay(
            "project",
            "experiment",
            "source",
            "variable",
            "date",
            access_type=aerovaldb.AccessType.BLOB,
        )
        assert data == input_data

        if isinstance(tmpdb, aerovaldb.jsondb.AerovalJsonFileDB):
            file_path: str = db.get_map_overlay(
                "project",
                "experiment",
                "source",
                "variable",
                "date",
                access_type=aerovaldb.AccessType.FILE_PATH,
            )
            assert file_path.endswith(expected_extension)


@TESTDB_PARAMETRIZATION
def test_get_map_filtering(testdb):
    with aerovaldb.open(testdb) as db:
        data = db.get_map(
            "project",
            "experiment",
            "network",
            "obsvar",
            "layer",
            "model",
            "modvar",
            "time2",
            frequency="frequency",
            season="season",
        )

    assert "frequency" in data[0]
    assert not "excluded_frequency" in data[0]
    assert "season" in data[0]["frequency"]
    assert not "excluded_season" in data[0]["frequency"]
