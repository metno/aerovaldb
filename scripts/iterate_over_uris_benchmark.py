import aerovaldb
import time
import logging

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

with aerovaldb.open("json_files:./tests/test-db/json") as db:
    start_time = time.perf_counter()
    count = len(list(db.list_all()))
    end_time = time.perf_counter()

print(f"json_files: {count} items in {end_time-start_time:.3f} seconds")

# 0.197 seconds

with aerovaldb.open("sqlitedb:./tests/test-db/sqlite/test.sqlite") as db:
    start_time = time.perf_counter()
    count = len(list(db.list_all()))
    end_time = time.perf_counter()

print(f"sqlite:     {count} items in {end_time-start_time:.3f} seconds")

# 0.001 seconds

with aerovaldb.open("json_files:./tests/test-db/json") as db:
    start_time = time.perf_counter()
    count = len(list(db.list_all()))
    end_time = time.perf_counter()

print(f"json_files: {count} items in {end_time-start_time:.3f} seconds (Cached)")
