import pytest

import aerovaldb
from aerovaldb.utils.copy import copy_db_contents


@pytest.mark.dependency(scope="session", depends=["test_list_all"])
def test_copy_json_to_json(tmp_path):
    path = str(tmp_path)

    with aerovaldb.open(f"json_files:tests/test-db/json") as source:
        with aerovaldb.open(f"json_files:{path}") as dest:
            copy_db_contents(source, dest)

            assert len(source.list_all()) == len(dest.list_all())


@pytest.mark.dependency(scope="session", depends=["test_list_all"])
def test_copy_json_to_sqlite():
    with aerovaldb.open(f"json_files:tests/test-db/json") as source:
        with aerovaldb.open(":memory:") as dest:
            copy_db_contents(source, dest)

            assert len(source.list_all()) == len(dest.list_all())


@pytest.mark.dependency(scope="session", depends=["test_list_all"])
@pytest.mark.xfail(reason="Missing one asset after copy (?)")
def test_copy_sqlite_to_json(tmp_path):
    path = str(tmp_path)

    with aerovaldb.open(f"sqlitedb:tests/test-db/sqlite/test.sqlite") as source:
        with aerovaldb.open(f"json_files:{path}") as dest:
            copy_db_contents(source, dest)

            assert len(source.list_all()) == len(dest.list_all())


@pytest.mark.dependency(scope="session", depends=["test_list_all"])
def test_copy_sqlite_to_sqlite():
    with aerovaldb.open(f"sqlitedb:tests/test-db/sqlite/test.sqlite") as source:
        with aerovaldb.open(":memory:") as dest:
            copy_db_contents(source, dest)

            assert len(source.list_all()) == len(dest.list_all())
