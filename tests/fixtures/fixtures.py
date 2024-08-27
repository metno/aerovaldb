import pytest
import aerovaldb

DBTYPE_PARAMETRIZATION = pytest.mark.parametrize(
    "dbtype", (pytest.param("json_files"), pytest.param("sqlitedb"))
)


@pytest.fixture
def tmpdb(tmp_path, dbtype: str) -> aerovaldb.AerovalDB:
    """Fixture encapsulating logic for each tested database connection to create
    a fresh, temporary database and connect to it."""
    if dbtype == "json_files":
        return aerovaldb.open(f"json_files:{str(tmp_path)}")
    elif dbtype == "sqlitedb":
        return aerovaldb.open(":memory:")

    assert False
