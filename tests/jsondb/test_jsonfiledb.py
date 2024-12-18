import datetime
import os

import pytest

import aerovaldb
from aerovaldb.jsondb.jsonfiledb import AerovalJsonFileDB


def test_jsonfiledb__get_uri_for_file(tmp_path):
    with aerovaldb.open(f"json_files:{str(tmp_path)}") as db:
        db: AerovalJsonFileDB
        assert (
            db._get_uri_for_file(str(tmp_path / "project/experiments.json"))
            == "/v0/experiments/project?version=0.0.1"
        )


def test_jsonfiledb_invalid_parameter_values():
    with aerovaldb.open("json_files:./tests/test-db/json") as db:
        with pytest.raises(ValueError) as e:
            db.get_config("/%&/())()", "test")

        assert "is not a valid file name component" in str(e.value)


def test_with_symlink():
    with aerovaldb.open("json_files:./tests/test-db/json") as db:
        data = db.get_config("linked-json-project", "experiment")

        assert data["path"] == "link"


def test_get_map_overlay():
    with aerovaldb.open("json_files:./tests/test-db/json") as db:
        path = db.get_map_overlay(
            "project",
            "experiment",
            "source",
            "variable",
            "date",
            access_type=aerovaldb.AccessType.FILE_PATH,
        )

        assert os.path.exists(path)


def test_put_map_overlay(tmp_path):
    # http://www.libpng.org/pub/png/spec/1.2/PNG-Structure.html#PNG-file-signature
    PNG_FILE_SIGNATURE = bytes([137, 80, 78, 71, 13, 10, 26, 10])
    with aerovaldb.open(f"json_files:{str(tmp_path)}") as db:
        db.put_map_overlay(
            PNG_FILE_SIGNATURE, "project", "experiment", "source", "variable", "date"
        )

        path: str = db.get_map_overlay(
            "project",
            "experiment",
            "source",
            "variable",
            "date",
            access_type=aerovaldb.AccessType.FILE_PATH,
        )
        assert os.path.exists(path)
        assert path.endswith(".png")

        read_bytes = db.get_map_overlay(
            "project",
            "experiment",
            "source",
            "variable",
            "date",
            access_type=aerovaldb.AccessType.BLOB,
        )

        assert read_bytes == PNG_FILE_SIGNATURE
