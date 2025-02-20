import pytest
from packaging.version import Version

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
                # Just a random hex sequence that doesn't match any known file headers
                # of filetype library.
                bytes.fromhex("6192d0f95dcbe642"),
                "project",
                "experiment",
                "source",
                "variable",
                "date",
            )

        assert "Could not guess image file extension" in str(e.value)


async def mock_version_provider(self, project: str, experiment: str):
    # Mocks version to a version where backwards compatibility is relevant for the below tests.
    return Version("0.25.0")


def test_get_uri_with_dashes(tmp_path, mocker):
    mocker.patch.object(AerovalJsonFileDB, "_get_version", mock_version_provider)
    with aerovaldb.open(f"json_files:{tmp_path}") as db:
        db.put_map(
            {},
            "project",
            "experiment",
            "AERONET-Sun",
            "od550aer",
            "Column",
            "TM5-AP3-CTRL",
            "od550aer",
            "2010",
        )

        assert (
            db.list_all()[0]
            == "/v0/map/project/experiment/AERONET-Sun/od550aer/Column/TM5-AP3-CTRL/od550aer?time=2010&version=0.25.0"
        )


def test_get_uri_with_underscore_region1(tmp_path, mocker):
    mocker.patch.object(AerovalJsonFileDB, "_get_version", mock_version_provider)
    with aerovaldb.open(f"json_files:{tmp_path}") as db:
        db.put_timeseries(
            {},
            "project",
            "experiment",
            "Amsterdam_Island",
            "AERONET-Sun",
            "od550aer",
            "Column",
        )

        assert (
            db.list_all()[0]
            == "/v0/ts/project/experiment/Amsterdam%2Island/AERONET-Sun/od550aer/Column?version=0.25.0"
        )


def test_get_uri_with_underscore_region2(tmp_path, mocker):
    mocker.patch.object(AerovalJsonFileDB, "_get_version", mock_version_provider)
    with aerovaldb.open(f"json_files:{tmp_path}") as db:
        db.put_heatmap_timeseries(
            {},
            "project",
            "experiment",
            "some_region",
            "some-network",
            "obsvar",
            "layer",
        )

        assert (
            db.list_all()[0]
            == "/v0/hm_ts/project/experiment?region=some%2region&network=some-network&obsvar=obsvar&layer=layer&version=0.25.0"
        )
