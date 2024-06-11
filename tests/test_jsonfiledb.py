import asyncio
import os
import random

import pytest

import aerovaldb

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
        # ("get_experiments", ["project"], None, "./project/"),
        ("get_config", ["project", "experiment"], None, "./project/experiment/"),
        # ("get_menu", ["project", "experiment"], None, "./project/experiment/"),
        ("get_statistics", ["project", "experiment"], None, "./project/experiment/"),
        # ("get_ranges", ["project", "experiment"], None, "./project/experiment/"),
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
]


@pytest.mark.asyncio
@pytest.mark.parametrize("resource", (("json_files:./tests/test-db/json",)))
@pytest.mark.parametrize(*get_parameters)
async def test_getter(resource: str, fun: str, args: list, kwargs: dict, expected):
    """
    This test tests that data is read as expected from a static, fixed database.
    """
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
async def test_file_does_not_exist():
    with aerovaldb.open("json_files:./tests/test-db/json") as db:
        with pytest.raises(aerovaldb.FileDoesNotExist):
            await db.get_config(
                "non-existent-project",
                "experiment",
                access_type=aerovaldb.AccessType.FILE_PATH,
            )


set_parametrization = pytest.mark.parametrize(
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
        # ("experiments", ["project"], None),
        ("config", ["project", "experiment"], None),
        # ("menu", ["project", "experiment"], None),
        ("statistics", ["project", "experiment"], None),
        # ("ranges", ["project", "experiment"], None),
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
@set_parametrization
async def test_setters(fun: str, args: list, kwargs: dict, tmp_path):
    """
    This test tests that you read back the expected data, once you have written
    to a fresh db, assuming the same arguments.
    """
    with aerovaldb.open(f"json_files:{os.path.join(tmp_path, fun)}") as db:
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


@set_parametrization
def test_setters_sync(fun: str, args: list, kwargs: dict, tmp_path):
    """
    This test tests that you read back the expected data, once you have written
    to a fresh db, assuming the same arguments.
    """
    with aerovaldb.open(f"json_files:{os.path.join(tmp_path, fun)}") as db:
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


def test_exception_on_unexpected_args():
    """
    https://github.com/metno/aerovaldb/issues/19
    """
    with aerovaldb.open("json_files:./tests/test-db/json") as db:
        with pytest.raises(aerovaldb.UnusedArguments):
            db.get_config("project", "experiment", "excessive-positional-argument")


@pytest.mark.xfail
def test_exception_on_unexpected_kwargs():
    """
    https://github.com/metno/aerovaldb/issues/19
    """
    with aerovaldb.open("json_files:./tests/test-db/json") as db:
        with pytest.raises(ValueError):
            db.get_experiments("project", unused_kwarg="test")


def test_version1():
    """ """
    with aerovaldb.open("json_files:./tests/test-db/json") as db:
        assert str(db._get_version("project", "experiment")) == "0.13.5"


def test_version2():
    """ """
    with aerovaldb.open("json_files:./tests/test-db/json") as db:
        assert str(db._get_version("project", "experiment-old")) == "0.0.5"


def test_list_experiments():
    with aerovaldb.open("json_files:./tests/test-db/json") as db:
        experiments = db._list_experiments("project")
        assert set(experiments) == set(["experiment", "experiment-old"])
