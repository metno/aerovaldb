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


def test_with_symlink():
    with aerovaldb.open("json_files:./tests/test-db/json") as db:
        data = db.get_config("linked-json-project", "experiment")

        assert data["path"] == "link"


def test_put_map_overlay_extension_guess_error(tmp_path):
    with aerovaldb.open(f"json_files:{str(tmp_path)}") as db:
        db: AerovalJsonFileDB

        with pytest.raises(ValueError) as e:
            db.put_map_overlay(
                bytes.fromhex("6192d0f95dcbe642"),
                "project",
                "experiment",
                "source",
                "variable",
                "date",
            )

        assert "Could not guess image file extension" in str(e.value)
