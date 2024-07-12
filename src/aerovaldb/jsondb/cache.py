from collections import defaultdict, deque
from pathlib import Path
from ..utils import async_and_sync
import logging
import os
import aiofile
from typing import TypedDict, Hashable

logger = logging.getLogger(__name__)


class LRUQueue:
    """
    Small helper class that efficiently maintains a LRUQueue
    by combining a set and deque to maintain a unique constraint
    on the queue. Re-adding an element will return it to the
    end of the queue.
    """

    def __init__(self):
        self._set = set()
        self._deque = deque()

    @property
    def size(self) -> int:
        """Returns the lenth of the queue."""
        return len(self._set)

    def add(self, item: Hashable):
        """
        Adds an item to the queue.
        """
        if item in self._set:
            self._deque.remove(item)

        self._set.add(item)
        self._deque.appendleft(item)

    def pop(self) -> Hashable:
        """Removes and returns the top item from the queue."""
        item = self._deque.pop()
        self._set.remove(item)
        return item

    def remove(self, item: Hashable):
        """
        Removes an item from the queue.
        """
        if item in self._set:
            self._set.remove(item)
            self._deque.remove(item)


class CacheEntry(TypedDict):
    json: str

    last_modified: float


class JSONLRUCache:
    """
    Implements an in-memory LRU cache for file content in aerovaldb.
    """

    def __init__(self, *, max_size: int, asyncio: bool = False):
        """
        :param max_size : The maximum size of the cache in terms of number of entries / files.

        Files will be ejected based on least recently used, when full.
        """
        self._asyncio = asyncio
        self._max_size = max_size
        self.invalidate_all()

    def invalidate_all(self) -> None:
        logger.debug("JSON Cache invalidated.")

        # Stores the actual cached content, indexed by canonical file path.
        self._cache: defaultdict[str, CacheEntry | None] = defaultdict(lambda: None)

        # Stores queue of cache accesses, used for implementing LRU logic.
        self._queue = LRUQueue()

        # Tally of cache hits and misses.
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
        return self._queue.size

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
        if self._asyncio:
            async with aiofile.async_open(abspath, "r") as f:
                return await f.read()

        with open(abspath, "r") as f:
            return f.read()

    def _get(self, abspath: str) -> str:
        """Returns an element from the cache."""
        self._queue.add(abspath)
        self._hit_count = self._hit_count + 1
        logger.debug(f"Returning contents from file {abspath} from cache.")
        return self._cache[abspath]["json"]  # type: ignore

    def _put(self, abspath: str, *, json: str, modified: float):
        self._cache[abspath] = {
            "json": json,
            "last_modified": os.path.getmtime(abspath),
        }
        while self.size > self._max_size:
            key = self._queue.pop()
            self.invalidate_entry(str(key))

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
        self._queue.add(abspath)
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
            self._queue.remove(abspath)

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
