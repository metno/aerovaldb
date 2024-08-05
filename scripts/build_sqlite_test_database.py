import os
import aerovaldb

# This script is a helper script to create an sqlite database
# with the same contents as the json test database. It does so
# by copying each asset from the jsondb into test.sqlite

if os.path.exists("tests/test-db/sqlite/test.sqlite"):
    os.remove("tests/test-db/sqlite/test.sqlite")

jsondb = aerovaldb.open("json_files:tests/test-db/json")
sqlitedb = aerovaldb.open("sqlitedb:tests/test-db/sqlite/test.sqlite")

for i, uri in enumerate(list(jsondb.list_all())):
    print(f"[{i}] - Processing uri {uri}")
    data = jsondb.get_by_uri(
        uri, access_type=aerovaldb.AccessType.JSON_STR, default="{}"
    )
    sqlitedb.put_by_uri(data, uri)

# json_list = list(jsondb.list_all())
# sqlite_list = list(sqlitedb.list_all())
# print("The following URIs exist in jsondb but not sqlitedb")
# for x in json_list:
#    if not (x in sqlite_list):
#        print(x)
#
# print("The following URIs exist in sqlitedb but not jsondb")
#
# for x in sqlite_list:
#    if not (x in json_list):
#        print(x)

print(f"jsondb number of assets: {len(list(jsondb.list_all()))}")
print(f"sqlite number of assets: {len(list(sqlitedb.list_all()))}")
