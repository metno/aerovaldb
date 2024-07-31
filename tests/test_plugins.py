import pytest
import os

import aerovaldb
from aerovaldb.jsondb.jsonfiledb import AerovalJsonFileDB
from aerovaldb.sqlitedb.sqlitedb import AerovalSqliteDB


def test_plugins():
    engines = aerovaldb.list_engines()
    print(engines)
    assert len(engines) == 2


def test_open_json_1():
    with aerovaldb.open("json_files:.") as db:
        assert isinstance(db, AerovalJsonFileDB)
        assert os.path.realpath(db._basedir) == os.path.realpath(".")


def test_open_json_2():
    with aerovaldb.open(".") as db:
        assert isinstance(db, AerovalJsonFileDB)
        assert os.path.realpath(db._basedir) == os.path.realpath(".")


@pytest.mark.parametrize(
    "fext", (pytest.param(".sqlite", id="sqlite"), pytest.param(".db", id="db"))
)
def test_open_sqlite_1(tmp_path, fext):
    path = os.path.join(tmp_path, f"test{fext}")
    with aerovaldb.open(f"sqlitedb:{path}") as db:
        assert isinstance(db, AerovalSqliteDB)
        assert db._dbfile == path


@pytest.mark.parametrize(
    "fext", (pytest.param(".sqlite", id="sqlite"), pytest.param(".db", id="db"))
)
def test_open_sqlite_2(tmp_path, fext):
    path = os.path.join(tmp_path, f"test{fext}")
    with aerovaldb.open(path) as db:
        assert isinstance(db, AerovalSqliteDB)
        assert db._dbfile == path
