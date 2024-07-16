import pytest
from multiprocessing import Process

import asyncio
from aerovaldb.jsondb.lock import JsonDbLock
import aerovaldb
from pathlib import Path
import logging

pytest_plugins = ("pytest_asyncio",)

logger = logging.getLogger(__name__)


@pytest.mark.asyncio
async def test_simple_locking(tmp_path):
    lock = JsonDbLock(tmp_path / "lock")

    assert not lock.has_lock()

    await lock.acquire()
    assert lock.has_lock()

    lock.release()
    assert not lock.has_lock()


@pytest.mark.asyncio
async def test_multiprocess_locking(monkeypatch, tmp_path):
    COUNTER_LIST = [100, 200, 150, 300, 400, 200]
    # monkeypatch.setenv("AVDB_LOCK_DIR", tmp_path / "lock")
    data_file = Path(tmp_path / "data")

    async def increment(n: int):
        with aerovaldb.open(f"json_files:{tmp_path}") as db:
            for i in range(n):
                await db.acquire_lock()
                data = await db.get_by_uuid(data_file, default={"counter": 0})
                data["counter"] += 1
                await db.put_by_uuid(data, data_file)
                db.release_lock()

    def helper(x):
        asyncio.run(increment(x))

    processes: list[Process] = []

    for n in COUNTER_LIST:
        p = Process(target=helper, args=(n,))
        processes.append(p)
        p.start()

    [p.join() for p in processes]

    with aerovaldb.open(f"json_files:{tmp_path}") as db:
        data = await db.get_by_uuid(str(data_file))

    assert data["counter"] == sum(COUNTER_LIST)
