import pytest
import aerovaldb


@pytest.mark.parametrize("resource", (("json_files:./tests/test-db/json",)))
@pytest.mark.parametrize(
    "fun,args,expected",
    (
        (
            "get_glob_stats",
            ["project", "experiment", "frequency"],
            "./project/experiment/hm/",
        ),
        (
            "get_contour",
            ["project", "experiment", "modvar", "model"],
            "./project/experiment/contour/",
        ),
        (
            "get_ts",
            ["project", "experiment", "region", "network", "obsvar", "layer"],
            "./project/experiment/ts/",
        ),
        (
            "get_ts_weekly",
            ["project", "experiment", "region", "network", "obsvar", "layer"],
            "./project/experiment/ts/dirunal/",
        ),
        ("get_experiments", ["project"], "./project/"),
        ("get_config", ["project", "experiment"], "./project/experiment/"),
        ("get_menu", ["project", "experiment"], "./project/experiment/"),
        ("get_statistics", ["project", "experiment"], "./project/experiment/"),
        ("get_ranges", ["project", "experiment"], "./project/experiment/"),
        ("get_regions", ["project", "experiment"], "./project/experiment/"),
        # TODO: /model_style and /map when optional parameter handling is decided / implemented.
        (
            "get_ts_weekly",
            ["project", "experiment", "region", "network", "obsvar", "layer"],
            "./project/experiment/ts/dirunal/",
        ),
        (
            "get_scat",
            ["project", "experiment", "network", "obsvar", "layer", "model", "modvar"],
            "./project/experiment/profiles/",
        ),
        (
            "get_profiles",
            ["project", "experiment", "region", "network", "obsvar"],
            "./project/experiment/profiles/",
        ),
    ),
)
def test_getter(resource: str, fun: str, args: list, expected):
    with aerovaldb.open(resource) as db:
        f = getattr(db, fun)

        data = f(*args)

        assert data["path"] == expected


def test_put_glob_stats():
    # TODO: These tests should ideally cleanup after themselves. For now
    # it is best to delete ./tests/test-db/tmp before running to verify
    # that they run as intended.
    with aerovaldb.open("json_files:./tests/test-db/tmp") as db:
        obj = {"data": "gibberish"}
        db.put_glob_stats(obj, "test1", "test2", "test3")
        read_data = db.get_glob_stats("test1", "test2", "test3")

        assert obj["data"] == read_data["data"]


def test_put_contour():
    with aerovaldb.open("json_files:./tests/test-db/tmp") as db:
        obj = {"data": "gibberish"}
        db.put_contour(obj, "test1", "test2", "test3", "test4")
        read_data = db.get_contour("test1", "test2", "test3", "test4")
        assert obj["data"] == read_data["data"]


def test_put_ts():
    with aerovaldb.open("json_files:./tests/test-db/tmp") as db:
        obj = {"data": "gibberish"}
        db.put_ts(obj, "test1", "test2", "test3", "test4", "test5", "test6")

        read_data = db.get_ts("test1", "test2", "test3", "test4", "test5", "test6")

        assert obj["data"] == read_data["data"]
