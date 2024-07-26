import pytest
import os

import aerovaldb
from aerovaldb.jsondb.jsonfiledb import AerovalJsonFileDB


def test_plugins():
    engines = aerovaldb.list_engines()
    print(engines)
    assert len(engines) >= 1


def test_open_1():
    with aerovaldb.open("json_files:.") as db:
        assert isinstance(db, AerovalJsonFileDB)
        assert os.path.realpath(db._basedir) == os.path.realpath(".")


def test_open_2():
    with aerovaldb.open(".") as db:
        assert isinstance(db, AerovalJsonFileDB)
        assert os.path.realpath(db._basedir) == os.path.realpath(".")
