from collections import defaultdict, deque
from pathlib import Path
from ..utils import async_and_sync
import logging
import os
import aiofile
import time
from typing import TypedDict

logger = logging.getLogger(__name__)


class CacheEntry(TypedDict):
    json: str

    last_modified: float


class JSONLRUCache:
    """
    Implements an in-memory LRU cache for file content in aerovaldb.
    """

    def __init__(self, *, max_size: int):
        """
        :param max_size : The maximum size of the cache in terms of number of entries / files.

        Files will be ejected based on least recently used, when full.
        """
        self._max_size = max_size
        self.invalidate_all()

    def invalidate_all(self) -> None:
        logger.debug("JSON Cache invalidated.")
        self._cache: defaultdict[str, CacheEntry | None] = defaultdict(lambda: None)
        self._deque: deque = deque()
        self._hit_count: int = 0
        self._miss_count: int = 0

    @property
    def hit_count(self) -> int:
        """Returns the number of cache hits since the last `invalidate_all()` call.

        Note:
        -----
        This does not include calls with `no_cache=True`
        """
        return self._hit_count

    @property
    def size(self) -> int:
        """Returns the current size of the cache in terms of number of elements."""
        return len(self._cache)

    @property
    def miss_count(self) -> int:
        """Returns the number of cache misses since the last `invalidate_all()` call.

        Note:
        -----
        This does not include calls with `no_cache=True`
        """
        return self._miss_count

    def _canonical_file_path(self, file_path: str | Path) -> str:
        """
        Returns an absolute file path with symlinks removed for a file to
        ensure correct lookup.

        :param file_path : The file path.

        :return : The file path converted to canonical file path.
        """
        return str(os.path.realpath(file_path))

    async def _read_json(self, file_path: str | Path) -> str:
        abspath = self._canonical_file_path(file_path)
        logger.debug(f"Reading file {abspath}")
        async with aiofile.async_open(abspath, "r") as f:
            return await f.read()

    def _get(self, abspath: str) -> str:
        """Returns an element from the cache."""
        self._deque.remove(abspath)
        self._deque.append(abspath)
        self._hit_count = self._hit_count + 1
        logger.debug(f"Returning contents from file {abspath} from cache.")
        self._cache[abspath]["last_accessed"] = time.time()  # type: ignore
        return self._cache[abspath]["json"]  # type: ignore

    def _put(self, abspath: str, *, json: str, modified: float):
        self._cache[abspath] = {
            "json": json,
            "last_modified": os.path.getmtime(abspath),
        }
        while self.size > self._max_size:
            key = self._deque.popleft()
            self.invalidate_entry(key)

    @async_and_sync
    async def get_json(self, file_path: str | Path, *, no_cache: bool = False) -> str:
        """
        Fetches json a str from a file, using the cached version if it is still valid.

        :param file_path : The file path to be fetched.
        :no_cache : If true, file will always be read from the file.
        """
        abspath = self._canonical_file_path(file_path)
        if no_cache:
            return await self._read_json(abspath)

        if self.is_valid(abspath):
            return self._get(abspath)

        self._miss_count = self._miss_count + 1
        logger.debug(f"Reading file {abspath} and adding to cache.")
        json = await self._read_json(abspath)
        self._deque.append(abspath)
        self._put(abspath, json=json, modified=os.path.getmtime(abspath))
        return json

    def invalidate_entry(self, file_path: str | Path) -> None:
        """
        Invalidates the cache for a file path, ensuring it will be re-read on the next read.

        :param file_path : The file path to invalidate cache for.
        """
        abspath = self._canonical_file_path(file_path)
        logger.debug(f"Invalidating cache for file {abspath}.")
        if abspath in self._cache:
            del self._cache[abspath]
            try:
                self._deque.remove(abspath)
            except ValueError:
                pass

    def is_valid(self, file_path: str | Path) -> bool:
        """
        Checks whether a cache element is valid.

        :param file_path: The file path to check for.

        :returns : Boolean indicating cache validity.
        """
        abspath = self._canonical_file_path(file_path)

        cache = self._cache[abspath]
        if cache is None:
            return False

        if not os.path.exists(abspath):
            return False

        if os.path.getmtime(abspath) > cache["last_modified"]:
            return False

        return True
