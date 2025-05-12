import pathlib

import pytest
from packaging.version import Version

import aerovaldb
from aerovaldb.jsondb.jsonfiledb import AerovalJsonFileDB
from tests.test_aerovaldb import TEST_IMAGES


def test_jsonfiledb__get_uri_for_file(tmp_path):
    with aerovaldb.open(f"json_files:{str(tmp_path)}") as db:
        db: AerovalJsonFileDB
        assert (
            str(
                db._get_query_entry_for_file(str(tmp_path / "project/experiments.json"))
            )
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


@pytest.mark.parametrize(
    "uri,meta",
    (
        (
            "/v0/hm_ts/project/experiment?region=some_region&network=some-network&obsvar=obsvar&layer=layer&version=0.25.0",
            {
                "project": "project",
                "experiment": "experiment",
                "region": "some_region",
                "network": "some-network",
                "obsvar": "obsvar",
                "layer": "layer",
            },
        ),
        (
            "/v0/ts/project/experiment/Amsterdam_Island/AERONET-Sun/od550aer/Column?version=0.25.0",
            {
                "project": "project",
                "experiment": "experiment",
                "region": "Amsterdam_Island",
                "network": "AERONET-Sun",
                "obsvar": "od550aer",
                "layer": "Column",
            },
        ),
        (
            "/v0/map/project/experiment/AERONET-Sun/od550aer/Column/TM5-AP3-CTRL/od550aer?time=2010&version=0.25.0",
            {
                "project",
                "experiment",
                "AERONET-Sun",
                "od550aer",
                "Column",
                "TM5-AP3-CTRL",
                "od550aer",
                "2010",
            },
        ),
    ),
)
def test_backwards_compatibility_uri(tmp_path, mocker, uri: str, meta: dict[str, str]):
    mocker.patch.object(AerovalJsonFileDB, "_get_version", mock_version_provider)
    with aerovaldb.open(f"json_files:{tmp_path}") as db:
        db.put_by_uri({}, uri)

        assert str(db.list_all()[0]) == uri


def test_overlays_encoding(tmp_path):
    dat = open(TEST_IMAGES[".png"], "rb").read()
    with aerovaldb.open(f"json_files:{tmp_path}") as db:
        db.put_map_overlay(dat, "FFire", "FFire2022_eea", "source", "variable", "date")

        assert (
            pathlib.Path(tmp_path)
            / "FFire/FFire2022%2eea/overlay/variable_source/variable_source_date.png"
        ).exists()

        db.get_map_overlay("FFire", "FFire2022%2eea", "source", "variable", "date")
