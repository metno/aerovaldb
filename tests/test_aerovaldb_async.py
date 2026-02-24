# This test file applies tests to each implementation of database connector
# through the interface defined in src/aerovaldb/aerovaldb.py
# In addition each implementation has implementation specific tests in its
# respective test_*.py file
# - json_files: tests/jsondb/test_jsonfiledb.py
# - sqlitedb:   tests/sqlitedb/test_sqlitedb.py

import asyncio
import random

import pytest

import aerovaldb
from aerovaldb.utils.asyncio import has_async_loop
from .fixtures_aerovaldb import (
    GET_PARAMETRIZATION,
    IMPLEMENTATION_PARAMETRIZATION,
    PUT_PARAMETRIZATION,
    TESTDB_PARAMETRIZATION,
    tmpdb
)

# create a single event loop for all tests in this file
pytestmark = pytest.mark.asyncio(loop_scope="module")
loop: asyncio.AbstractEventLoop

async def test_remember_loop():
    global loop
    loop = asyncio.get_running_loop()


@TESTDB_PARAMETRIZATION
@GET_PARAMETRIZATION
async def test_getter(testdb: str, fun: str, args: list, kwargs: dict, expected):
    """
    This test tests that data is read as expected from a static, fixed database.
    """
    global loop
    assert asyncio.get_running_loop() is loop
    assert has_async_loop() is True
    with aerovaldb.open(testdb) as db:
        f = getattr(db, fun)

        if kwargs is not None:
            data = await f(*args, **kwargs)
        else:
            data = await f(*args)

        assert data["path"] == expected


@IMPLEMENTATION_PARAMETRIZATION
@PUT_PARAMETRIZATION
async def test_setters(dbtype: str, fun: str, args: list, kwargs: dict, tmpdb):
    """
    This test tests that you read back the expected data, once you have written
    to a fresh db, assuming the same arguments.
    """
    global loop
    assert asyncio.get_running_loop() is loop
    assert has_async_loop() is True
    with tmpdb as db:
        get = getattr(db, f"get_{fun}")
        put = getattr(db, f"put_{fun}")

        expected = fun + str(random.randint(0, 100000))
        if kwargs is not None:
            await put({"data": expected}, *args, **kwargs)

            data = await get(*args, **kwargs)
        else:
            await put({"data": expected}, *args)

            data = await get(*args)

        assert data["data"] == expected


@TESTDB_PARAMETRIZATION
async def test_file_does_not_exist(testdb):
    global loop
    assert asyncio.get_running_loop() is loop
    assert has_async_loop() is True
    with aerovaldb.open(testdb) as db:
        with pytest.raises(FileNotFoundError):
            await db.get_config(
                "non-existent-project",
                "experiment",
            )
