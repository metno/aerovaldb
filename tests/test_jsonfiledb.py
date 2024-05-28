import pytest
import aerovaldb
import asyncio

pytest_plugins = ("pytest_asyncio",)

get_parameters = [
    "fun,args,kwargs,expected",
    (
        (
            "get_glob_stats",
            ["project", "experiment", "frequency"],
            None,
            "./project/experiment/hm/",
        ),
        (
            "get_contour",
            ["project", "experiment", "modvar", "model"],
            None,
            "./project/experiment/contour/",
        ),
        (
            "get_timeseries",
            ["project", "experiment", "region", "network", "obsvar", "layer"],
            None,
            "./project/experiment/ts/",
        ),
        (
            "get_timeseries_weekly",
            ["project", "experiment", "region", "network", "obsvar", "layer"],
            None,
            "./project/experiment/ts/dirunal/",
        ),
        ("get_experiments", ["project"], None, "./project/"),
        ("get_config", ["project", "experiment"], None, "./project/experiment/"),
        ("get_menu", ["project", "experiment"], None, "./project/experiment/"),
        ("get_statistics", ["project", "experiment"], None, "./project/experiment/"),
        ("get_ranges", ["project", "experiment"], None, "./project/experiment/"),
        ("get_regions", ["project", "experiment"], None, "./project/experiment/"),
        ("get_models_style", ["project"], None, "./project/"),
        (
            "get_models_style",
            ["project"],
            {"experiment": "experiment"},
            "./project/experiment/",
        ),
        (
            "get_map",
            ["project", "experiment", "network", "obsvar", "layer", "model", "modvar"],
            None,
            "./project/experiment/map/",
        ),
        (
            "get_map",
            ["project", "experiment", "network", "obsvar", "layer", "model", "modvar"],
            {"time": "time"},
            "./project/experiment/map/with_time",
        ),
        (
            "get_timeseries_weekly",
            ["project", "experiment", "region", "network", "obsvar", "layer"],
            None,
            "./project/experiment/ts/dirunal/",
        ),
        (
            "get_scatter",
            ["project", "experiment", "network", "obsvar", "layer", "model", "modvar"],
            None,
            "./project/experiment/scat/",
        ),
        (
            "get_scatter",
            ["project", "experiment", "network", "obsvar", "layer", "model", "modvar"],
            {"time": "time"},
            "./project/experiment/scat/time",
        ),
        (
            "get_profiles",
            ["project", "experiment", "region", "network", "obsvar"],
            None,
            "./project/experiment/profiles/",
        ),
        (
            "get_heatmap_timeseries",
            ["project", "experiment"],
            None,
            "project/experiment/hm/ts/stats_ts.json",
        ),
        (
            "get_heatmap_timeseries",
            ["project", "experiment"],
            {
                "network": "network",
                "obsvar": "obsvar",
                "layer": "layer",
            },
            "./project/experiment/hm/ts/network-obsvar-layer",
        ),
        (
            "get_heatmap_timeseries",
            ["project", "experiment"],
            {
                "network": "network",
                "obsvar": "obsvar",
                "layer": "layer",
                "station": "region",
            },
            "./project/experiment/hm/ts/",
        ),
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
]

@pytest.mark.asyncio
@pytest.mark.parametrize("resource", (("json_files:./tests/test-db/json",)))
@pytest.mark.parametrize(*get_parameters)
async def test_getter(resource: str, fun: str, args: list, kwargs: dict, expected):
    with aerovaldb.open(resource) as db:
        f = getattr(db, fun)

        if kwargs is not None:
            data = await f(*args, **kwargs)
        else:
            data = await f(*args)

        assert data["path"] == expected


@pytest.mark.parametrize("resource", (("json_files:./tests/test-db/json",)))
@pytest.mark.parametrize(*get_parameters)
def test_getter_sync(resource: str, fun: str, args: list, kwargs: dict, expected):
    with aerovaldb.open(resource) as db:
        f = getattr(db, fun)

        if kwargs is not None:
            data = f(*args, **kwargs)
        else:
            data = f(*args)

        assert data["path"] == expected


@pytest.mark.asyncio
async def test_put_glob_stats():
    # TODO: These tests should ideally cleanup after themselves. For now
    # it is best to delete ./tests/test-db/tmp before running to verify
    # that they run as intended.
    with aerovaldb.open("json_files:./tests/test-db/tmp") as db:
        obj = {"data": "gibberish"}
        db.put_glob_stats(obj, "test1", "test2", "test3")
        read_data = await db.get_glob_stats("test1", "test2", "test3")

        assert obj["data"] == read_data["data"]


@pytest.mark.asyncio
async def test_put_contour():
    with aerovaldb.open("json_files:./tests/test-db/tmp") as db:
        obj = {"data": "gibberish"}
        db.put_contour(obj, "test1", "test2", "test3", "test4")
        read_data = await db.get_contour("test1", "test2", "test3", "test4")
        assert obj["data"] == read_data["data"]


@pytest.mark.asyncio
async def test_put_timeseries():
    with aerovaldb.open("json_files:./tests/test-db/tmp") as db:
        obj = {"data": "gibberish"}
        db.put_timeseries(obj, "test1", "test2", "test3", "test4", "test5", "test6")

        read_data = await db.get_timeseries(
            "test1", "test2", "test3", "test4", "test5", "test6"
        )

        assert obj["data"] == read_data["data"]


@pytest.mark.asyncio
async def test_file_does_not_exist():
    with aerovaldb.open("json_files:./tests/test-db/json") as db:
        with pytest.raises(aerovaldb.FileDoesNotExist):
            await db.get_experiments("non-existent-project", access_type = aerovaldb.AccessType.FILE_PATH)
