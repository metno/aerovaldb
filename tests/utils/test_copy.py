import aerovaldb
import pytest
from aerovaldb.utils.copy import copy_db_contents


def test_copy_json_to_json(tmp_path):
    path = str(tmp_path)

    with aerovaldb.open(f"json_files:tests/test-db/json") as source:
        with aerovaldb.open(f"json_files:{path}") as dest:
            copy_db_contents(source, dest)

            assert len(source.list_all()) == len(dest.list_all())


def test_copy_json_to_sqlite():
    with aerovaldb.open(f"json_files:tests/test-db/json") as source:
        with aerovaldb.open(":memory:") as dest:
            copy_db_contents(source, dest)

            assert len(source.list_all()) == len(dest.list_all())


def test_copy_sqlite_to_json(tmp_path):
    path = str(tmp_path)

    with aerovaldb.open(f"sqlitedb:tests/test-db/sqlite/test.sqlite") as source:
        with aerovaldb.open(f"json_files:{path}") as dest:
            copy_db_contents(source, dest)

            print("********")
            print(source.list_all())
            print(dest.list_all())
            assert len(source.list_all()) == len(dest.list_all())


def test_copy_sqlite_to_sqlite():
    with aerovaldb.open(f"sqlitedb:tests/test-db/sqlite/test.sqlite") as source:
        with aerovaldb.open(":memory:") as dest:
            copy_db_contents(source, dest)

            assert len(source.list_all()) == len(dest.list_all())