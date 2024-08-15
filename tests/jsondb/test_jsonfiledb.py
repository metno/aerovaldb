import pytest
import aerovaldb
from aerovaldb.jsondb.jsonfiledb import AerovalJsonFileDB


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
        assert set(experiments) == set(
            ["experiment", "experiment-old", "empty-experiment"]
        )


def test_list_experiments_results_only():
    with aerovaldb.open("json_files:./tests/test-db/json") as db:
        experiments = db._list_experiments("project", has_results=True)
        assert set(experiments) == set(["experiment", "experiment-old"])


def test_get_experiments():
    with aerovaldb.open("json_files:./tests/test-db/json") as db:
        experiments = db.get_experiments("project-no-experiments-json")

        assert experiments == {
            "experiment": {"public": True},
        }


def test_list_timeseries():
    with aerovaldb.open("json_files:./tests/test-db/json") as db:
        timeseries = db.list_timeseries("project", "experiment")

        assert len(list(timeseries)) == 1


def test_list_glob_stats():
    with aerovaldb.open("json_files:./tests/test-db/json") as db:
        glob_stats = list(db.list_glob_stats("project", "experiment"))

        assert len(glob_stats) == 1


def test_jsonfiledb__get_uri_for_file(tmp_path):
    with aerovaldb.open(f"json_files:{str(tmp_path)}") as db:
        db: AerovalJsonFileDB
        assert (
            db._get_uri_for_file(str(tmp_path / "project/experiments.json"))
            == "/v0/experiments/project"
        )


def test_jsonfiledb_invalid_parameter_values():
    with aerovaldb.open("json_files:./tests/test-db/json") as db:
        with pytest.raises(ValueError) as e:
            db.get_config("/%&/())()", "test")

        assert "is not a valid file name component" in str(e.value)
