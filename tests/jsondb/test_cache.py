import pytest
import os
from pathlib import Path
from aerovaldb.jsondb.cache import JSONCache
import time

pytest_plugins = ("pytest_asyncio",)


@pytest.fixture
def cache() -> JSONCache:
    return JSONCache()


@pytest.fixture
def test_files(tmpdir: Path) -> list[str]:
    """
    Generates a set of files with known contents for the cache
    to be tested on.
    """
    files = []
    for fname in ["test0.json", "test1.json", "test2.json"]:
        path = os.path.join(tmpdir, fname)

        with open(path, "w") as f:
            f.write(fname)

        files.append(path)

    return files


@pytest.mark.asyncio
async def test_cache(cache: JSONCache, test_files: list[str]):
    """
    Tests basic cache behaviour (ie. is the second call cached)
    """
    json = await cache.get_json(test_files[0])
    assert len(cache._cache) == 1
    assert cache.hit_count == 0
    assert cache.miss_count == 1
    assert json == "test0.json"

    json = await cache.get_json(test_files[0])
    assert len(cache._cache) == 1
    assert cache.hit_count == 1
    assert cache.miss_count == 1
    assert json == "test0.json"


async def test_change_mtime(cache: JSONCache, test_files: list[str]):
    """
    Test that cache is correctly invalidated when mtime of files
    is changed.
    """
    await cache.get_json(test_files[0])

    time.sleep(0.001)
    Path(test_files[0]).touch()

    json = await cache.get_json(test_files[0])
    assert cache.hit_count == 0
    assert cache.miss_count == 2
    assert json == "test0.json"


@pytest.mark.asyncio
async def test_manual_invalidation(cache: JSONCache, test_files: list[str]):
    """Tests that cache is correctly considered invalidated when
    `invalidate_cache()` is called on an entry."""
    await cache.get_json(test_files[0])

    cache.invalidate_cache(test_files[0])

    json = await cache.get_json(test_files[0])
    assert cache.hit_count == 0
    assert cache.miss_count == 2
    assert json == "test0.json"
