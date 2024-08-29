import os
import aerovaldb
from aerovaldb.utils.copy import copy_db_contents


if os.path.exists("tests/test-db/sqlite/test.sqlite"):
    os.remove("tests/test-db/sqlite/test.sqlite")

jsondb = aerovaldb.open("json_files:tests/test-db/json")
sqlitedb = aerovaldb.open("sqlitedb:tests/test-db/sqlite/test.sqlite")

copy_db_contents(jsondb, sqlitedb)  # type:ignore

print(f"jsondb number of assets: {len(jsondb.list_all())}")  # type: ignore
print(f"sqlite number of assets: {len(sqlitedb.list_all())}")  # type: ignore
