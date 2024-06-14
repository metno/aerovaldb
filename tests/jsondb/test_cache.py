import pytest
import os
from pathlib import Path
from aerovaldb.jsondb.cache import JSONLRUCache
import time

pytest_plugins = ("pytest_asyncio",)


@pytest.fixture
def cache() -> JSONLRUCache:
    return JSONLRUCache(max_size=2)


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
async def test_cache(cache: JSONLRUCache, test_files: list[str]):
    """
    Tests basic cache behaviour (ie. is the second call cached)
    """
    assert await cache.get_json(test_files[0]) == "test0.json"
    assert len(cache._cache) == 1
    assert cache.hit_count == 0
    assert cache.miss_count == 1

    assert await cache.get_json(test_files[0]) == "test0.json"
    assert len(cache._cache) == 1
    assert cache.hit_count == 1
    assert cache.miss_count == 1
    assert cache.size == 1


@pytest.mark.asyncio
async def test_change_mtime(cache: JSONLRUCache, test_files: list[str]):
    """
    Test that cache is correctly invalidated when mtime of files
    is changed.
    """
    await cache.get_json(test_files[0])

    time.sleep(0.005)
    Path(test_files[0]).touch()

    assert await cache.get_json(test_files[0]) == "test0.json"
    assert cache.hit_count == 0
    assert cache.miss_count == 2

    assert cache.size == 1


@pytest.mark.asyncio
async def test_manual_invalidation(cache: JSONLRUCache, test_files: list[str]):
    """Tests that cache is correctly considered invalidated when
    `invalidate_cache()` is called on an entry."""
    await cache.get_json(test_files[0])

    cache.invalidate_entry(test_files[0])
    assert cache.size == 0

    assert await cache.get_json(test_files[0]) == "test0.json"
    assert cache.hit_count == 0
    assert cache.miss_count == 2

    assert cache.size == 1


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "max_size,file_order,json,size,miss,hit",
    (
        (
            1,
            (0, 1, 2),
            ("test0.json", "test1.json", "test2.json"),
            (1, 1, 1),
            (1, 2, 3),
            (0, 0, 0),
        ),
        (
            1,
            (0, 0, 0),
            ("test0.json", "test0.json", "test0.json"),
            (1, 1, 1),
            (1, 1, 1),
            (0, 1, 2),
        ),
        (
            2,
            (0, 1, 0, 1, 0, 1),
            ("test0.json", "test1.json", "test0.json", "test1.json"),
            (1, 2, 2, 2),
            (1, 2, 2, 2),
            (0, 0, 1, 2),
        ),
        (
            2,
            (0, 1, 2, 0, 1, 2),
            (
                "test0.json",
                "test1.json",
                "test2.json",
                "test0.json",
                "test1.json",
                "test2.json",
            ),
            (1, 2, 2, 2, 2, 2),
            (1, 2, 3, 4, 5, 6),
            (0, 0, 0, 0, 0, 0),
        ),
    ),
)
async def test_lru_cache(
    test_files: list[str],
    max_size: int,
    file_order: list[int],
    json: list[str],
    size: list[int],
    miss: list[int],
    hit: list[int],
):
    """Tests cache lru ejection logic."""
    cache = JSONLRUCache(max_size=max_size)

    for file, js, sz, m, h in zip(file_order, json, size, miss, hit):
        path = test_files[file]

        assert await cache.get_json(path) == js
        assert cache.size == sz
        assert cache.miss_count == m
        assert cache.hit_count == h
