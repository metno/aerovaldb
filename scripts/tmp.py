import simplejson
import sqlite3


con = sqlite3.connect(":memory:")

data = simplejson.dumps({"test": 1234})

print(data)

cur = con.cursor()

cur.execute(
    """
    CREATE TABLE test(key, value)
    """
)

con.commit()

cur.execute(
    """
    INSERT INTO test
    VALUES(?, ?)
    """,
    ("test", data)
)

con.commit()

cur.execute(
    """
    SELECT value FROM test
    WHERE key='test'
    """
)
print(simplejson.loads(cur.fetchone()[0]))

import aerovaldb

with aerovaldb.open("tests/test-db/json") as db:
    print(data)
    
    #db.put_by_uri(data, "/v0/config/project/experiment")
    
    print(db.get_by_uri("/v0/config/project/experiment", access_type=aerovaldb.AccessType.JSON_STR))