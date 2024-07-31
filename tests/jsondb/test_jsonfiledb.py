import asyncio
import math
import os
import random

import pytest
import simplejson  # type: ignore
import aerovaldb


@pytest.mark.asyncio
async def test_file_does_not_exist():
    with aerovaldb.open("json_files:./tests/test-db/json") as db:
        with pytest.raises(FileNotFoundError):
            await db.get_config(
                "non-existent-project",
                "experiment",
                access_type=aerovaldb.AccessType.FILE_PATH,
            )


def test_write_and_read_of_nan(tmp_path):
    with aerovaldb.open(f"json_files:{tmp_path}") as db:
        data = dict(value=float("nan"))

        db.put_by_uri(data, "./test")

        read = db.get_by_uri("./test")

        # See Additional Notes on #59
        # https://github.com/metno/aerovaldb/pull/59
        assert read["value"] is None


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


def test_getter_with_default():
    with aerovaldb.open("json_files:./tests/test-db/json") as db:
        data = db.get_by_uri(
            "./project/experiment/non-existent-file.json", default={"data": "test"}
        )

        assert data["data"] == "test"


def test_getter_with_default_error():
    with aerovaldb.open("json_files:./tests/test-db/json") as db:
        with pytest.raises(simplejson.errors.JSONDecodeError):
            db.get_by_uri("./invalid-json.json", default={"data": "data"})
