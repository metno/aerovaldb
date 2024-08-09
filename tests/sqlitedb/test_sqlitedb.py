import aerovaldb
import pytest
import os
from aerovaldb.sqlitedb import AerovalSqliteDB


def test_db_initialization(tmp_path):
    file = os.path.join(tmp_path, "test.sqlite")
    with aerovaldb.open(file) as db:
        db: AerovalSqliteDB

        assert db._dbfile == file
        assert (
            db._get_metadata_by_key("created_by")
            == f"aerovaldb_{aerovaldb.__version__}"
        )
        assert (
            db._get_metadata_by_key("last_modified_by")
            == f"aerovaldb_{aerovaldb.__version__}"
        )

        # Check that all tables are properly initialized.
        cur = db._con.cursor()
        for table in AerovalSqliteDB.TABLE_NAME_LOOKUP.values():
            cur.execute(
                f"""
                PRAGMA table_info({table})
                """
            )
            assert cur.fetchone() is not None
