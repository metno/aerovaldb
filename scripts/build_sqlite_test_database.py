import os
import aerovaldb
import re


if os.path.exists("tests/test-db/sqlite/test.sqlite"):
    os.remove("tests/test-db/sqlite/test.sqlite")

jsondb = aerovaldb.open("json_files:tests/test-db/json")
sqlitedb = aerovaldb.open("sqlitedb:tests/test-db/sqlite/test.sqlite")

data = jsondb.get_config(
    "project", "experiment", access_type=aerovaldb.AccessType.FILE_PATH, default="{}"
)
print(data)
print(jsondb._get_uri_for_file(data))
print(
    jsondb.get_by_uri(
        jsondb._get_uri_for_file(data), access_type=aerovaldb.AccessType.JSON_STR
    )
)

sqlitedb.put_by_uri(data, jsondb._get_uri_for_file(data))

for i, uri in enumerate(list(jsondb.list_all())):
    print(f"Processing uri {uri}")
    data = jsondb.get_by_uri(
        uri, access_type=aerovaldb.AccessType.JSON_STR, default="{}"
    )
    sqlitedb.put_by_uri(data, uri)

print(f"jsondb number of assets: {len(list(jsondb.list_all()))}")
# print(f"sqlite number of assets: {len(list(sqlitedb.list_all()))}")
