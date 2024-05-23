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
        ("get_contour", ["project", "experiment"], "./project/experiment/contour/"),
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
    ),
)
def test_getter(resource: str, fun: str, args: list, expected):
    with aerovaldb.open(resource) as db:
        fun = getattr(db, fun)

        data = fun(*args)

        assert data["path"] == expected


def test_get_glob_stats():
    with aerovaldb.open(f"json_files:./tests/test-db/json") as db:
        data = db.get_glob_stats(
            "project", "experiment", "frequency", access_type="OBJ"
        )
        assert data["path"] == "./project/experiment/hm/"


def test_put_glob_stats():
    # TODO: These tests should ideally cleanup after themselves. For now
    # it is best to delete ./tests/test-db/tmp before running to verify
    # that they run as intended.
    with aerovaldb.open("json_files:./tests/test-db/tmp") as db:
        obj = {"data": "gibberish"}
        db.put_glob_stats(obj, "test1", "test2", "test3")
        read_data = db.get_glob_stats("test1", "test2", "test3")

        assert obj["data"] == read_data["data"]


def test_get_contour():
    with aerovaldb.open(f"json_files:./tests/test-db/json") as db:
        data = db.get_contour("project", "experiment")
        assert data["path"] == "./project/experiment/contour/"


def test_put_contour():
    with aerovaldb.open("json_files:./tests/test-db/tmp") as db:
        obj = {"data": "gibberish"}
        db.put_contour(obj, "test1", "test2", "test3", "test4")
        read_data = db.get_contour("test1", "test2", "test3", "test4")
        assert obj["data"] == read_data["data"]


def test_get_ts():
    with aerovaldb.open(f"json_files:./tests/test-db/json") as db:
        data = db.get_ts(
            "project", "experiment", "region", "network", "obsvar", "layer"
        )

        assert data["path"] == "./project/experiment/ts/"


def test_put_ts():
    with aerovaldb.open("json_files:./tests/test-db/tmp") as db:
        obj = {"data": "gibberish"}
        db.put_ts(obj, "test1", "test2", "test3", "test4", "test5", "test6")

        read_data = db.get_ts("test1", "test2", "test3", "test4", "test5", "test6")

        assert obj["data"] == read_data["data"]
