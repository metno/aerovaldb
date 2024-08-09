import os
import pytest
from multiprocessing import Process

import asyncio
from aerovaldb.lock.lock import FileLock, FakeLock
import aerovaldb
import logging

pytest_plugins = ("pytest_asyncio",)

logger = logging.getLogger(__name__)


def test_fake_lock():
    os.environ["AVDB_USE_LOCKING"] = "0"
    with aerovaldb.open("json_files:tests/test-db/json") as db:
        assert isinstance(db.lock(), FakeLock)


def test_file_lock():
    os.environ["AVDB_USE_LOCKING"] = "1"
    with aerovaldb.open("json_files:tests/test-db/json") as db:
        assert isinstance(db.lock(), FileLock)


@pytest.mark.asyncio
async def test_simple_locking(tmp_path):
    lock = FileLock(tmp_path / "lock")
    assert lock.is_locked()

    lock.release()
    assert not lock.is_locked()


@pytest.mark.asyncio
async def test_multiprocess_locking(monkeypatch, tmp_path):
    COUNTER_LIST = [100, 200, 150, 300, 400, 200]
    # monkeypatch.setenv("AVDB_LOCK_DIR", tmp_path / "lock")
    uri = "/v0/experiments/project"

    async def increment(n: int):
        with aerovaldb.open(f"json_files:{tmp_path}") as db:
            for i in range(n):
                with db.lock():
                    data = await db.get_by_uri(uri, default={"counter": 0})
                    data["counter"] += 1
                    await db.put_by_uri(data, uri)

    def helper(x):
        asyncio.run(increment(x))

    processes: list[Process] = []

    for n in COUNTER_LIST:
        p = Process(target=helper, args=(n,))
        processes.append(p)
        p.start()

    [p.join() for p in processes]

    with aerovaldb.open(f"json_files:{tmp_path}") as db:
        data = await db.get_by_uri(uri)

    assert data["counter"] == sum(COUNTER_LIST)
