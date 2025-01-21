# from typing import override # Supported with Python>= 3.12 only.
import logging
import os
from abc import ABC, abstractmethod
from collections import defaultdict, deque
from pathlib import Path
from typing import Hashable, TypedDict

from ..utils import async_and_sync

logger = logging.getLogger(__name__)


class CacheMissError(FileNotFoundError):
    pass


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


class BaseCache(ABC):
    def __init__(self):
        pass

    @property
    @abstractmethod
    def hit_count(self) -> int:
        raise NotImplementedError

    @property
    @abstractmethod
    def size(self) -> int:
        raise NotImplementedError

    @property
    @abstractmethod
    def miss_count(self) -> int:
        raise NotImplementedError

    @abstractmethod
    def get(self, key: str, *, bypass_cache: bool = False) -> str:
        raise NotImplementedError

    @abstractmethod
    def put(self, obj: str, *, key: str):
        raise NotImplementedError

    @abstractmethod
    def invalidate_all(self) -> None:
        raise NotImplementedError

    @abstractmethod
    def invalidate_entry(self, file_path: str) -> None:
        raise NotImplementedError

    @abstractmethod
    def is_valid(self, key: str) -> bool:
        raise NotImplementedError


class CacheEntry(TypedDict):
    json: str

    last_modified: float


class LRUFileCache(BaseCache):
    """
    Implements an in-memory LRU cache for file content.
    """

    def __init__(self, *, max_size: int):
        """
        :param max_size : The maximum size of the cache in terms of number of entries / files.

        Files will be ejected based on least recently used, when full.
        """
        self._hit_count: int = 0
        self._miss_count: int = 0
        self._max_size = max_size
        self.invalidate_all()

    def _get_entry(self, abspath: str):
        """Returns an element from the cache."""
        self._queue.add(abspath)
        self._hit_count = self._hit_count + 1
        logger.debug(f"Returning contents from file {abspath} from cache.")
        return self._entries[abspath]["json"]  # type: ignore

    # @override # Only supported with python >= 3.12 (https://peps.python.org/pep-0698/)
    def invalidate_all(self) -> None:
        logger.debug("JSON Cache invalidated.")

        # Stores the actual cached content, indexed by canonical file path.
        self._entries: defaultdict[str, CacheEntry | None] = defaultdict(lambda: None)

        # Stores queue of cache accesses, used for implementing LRU logic.
        self._queue = LRUQueue()

        # Tally of cache hits and misses.
        self._hit_count = 0
        self._miss_count = 0

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

    def _read_json(self, file_path: str | Path) -> str:
        abspath = self._canonical_file_path(file_path)
        logger.debug(f"Reading file {abspath}")

        with open(abspath, "r") as f:
            return f.read()

    def _put_entry(self, abspath: str, *, json: str):
        self._entries[abspath] = {
            "json": json,
            "last_modified": os.path.getmtime(abspath),
        }
        while self.size > self._max_size:
            key = self._queue.pop()
            self.invalidate_entry(str(key))

    # @override
    def get(self, key: str, *, bypass_cache: bool = False) -> str:
        abspath = self._canonical_file_path(key)

        if bypass_cache:
            return self._read_json(abspath)

        if self.is_valid(abspath):
            return self._get_entry(abspath)

        self._miss_count += 1
        json = self._read_json(abspath)
        self._queue.add(abspath)
        self._put_entry(abspath, json=json)
        return json

    # @override
    def put(self, obj, *, key: str):
        abspath = self._canonical_file_path(key)
        self._put_entry(abspath, json=obj)

    @async_and_sync
    # @override
    def get_json(self, file_path: str | Path, *, no_cache: bool = False) -> str:
        """
        Fetches json a str from a file, using the cached version if it is still valid.

        :param file_path : The file path to be fetched.
        :no_cache : If true, file will always be read from the file.
        """
        abspath = self._canonical_file_path(file_path)
        if no_cache:
            return self._read_json(abspath)

        if self.is_valid(abspath):
            return self._get_entry(abspath)

        self._miss_count = self._miss_count + 1
        logger.debug(f"Reading file {abspath} and adding to cache.")
        json = self._read_json(abspath)
        self._queue.add(abspath)
        self._put_entry(abspath, json=json)
        return json

    # @override
    def invalidate_entry(self, file_path: str | Path) -> None:
        """
        Invalidates the cache for a file path, ensuring it will be re-read on the next read.

        :param file_path : The file path to invalidate cache for.
        """
        abspath = self._canonical_file_path(file_path)
        logger.debug(f"Invalidating cache for file {abspath}.")
        if abspath in self._entries:
            del self._entries[abspath]
            self._queue.remove(abspath)

    # @override
    def is_valid(self, file_path: str | Path) -> bool:
        """
        Checks whether a cache element is valid.

        :param file_path: The file path to check for.

        :returns : Boolean indicating cache validity.
        """
        abspath = self._canonical_file_path(file_path)

        cache = self._entries[abspath]
        if cache is None:
            return False

        if not os.path.exists(abspath):
            return False

        if os.path.getmtime(abspath) > cache["last_modified"]:
            return False

        return True


class KeyCacheDecorator(BaseCache):
    """Decorator for other cache implementations which extends it with support for
    sub-parts of a json file.
    """

    def __init__(self, cache: BaseCache, *, max_size: int = 64):
        if not isinstance(cache, BaseCache):
            raise TypeError(f"Cache is of type {type(cache)}, expected BaseCache")

        self._cache = cache
        self.invalidate_all()

        self._max_size = max_size

    def _split_key(self, key: str) -> tuple[str, str | None]:
        splt = key.split("::")
        if len(splt) == 1:
            return (splt[0], None)
        elif len(splt) == 2:
            return tuple(splt)  # type: ignore

        raise ValueError(
            f"Unexpected number of elements in '{key}'. Expected 1 or 2, got {len(splt)}."
        )

    # @override
    @property
    def hit_count(self) -> int:
        return self._hit_count

    # @override
    @property
    def size(self) -> int:
        return self._queue.size

    # @override
    @property
    def miss_count(self) -> int:
        return self._miss_count

    # @override
    def get(self, key: str, *, bypass_cache: bool = False) -> str:
        fp, k = self._split_key(key)

        if k is None:
            return self._cache.get(fp, bypass_cache=bypass_cache)

        if self.is_valid(key):
            if (entry := self._entries[key]) is not None:
                self._hit_count += 1
                return entry["json"]

        self._miss_count += 1
        raise CacheMissError

    # @override
    def put(self, obj, *, key: str) -> None:
        fp, _ = self._split_key(key)
        self._entries[key] = {
            "json": obj,
            "last_modified": os.path.getmtime(fp),
        }
        self._queue.add(key)
        while self.size > self._max_size:
            key = self._queue.pop()  # type: ignore
            self.invalidate_entry(str(key))

    # @override
    def invalidate_all(self) -> None:
        self._entries: dict[str, CacheEntry | None] = defaultdict(lambda: None)
        self._miss_count = 0
        self._hit_count = 0
        self._queue = LRUQueue()

    # @override
    def invalidate_entry(self, key: str) -> None:
        logger.debug(f"Invalidating cache for key {key}.")
        if key in self._entries:
            del self._entries[key]
            self._queue.remove(key)

    # @override
    def is_valid(self, key: str) -> bool:
        fp, k = self._split_key(key)
        if k is None:
            # File access is delegated to sub-cache.
            return self._cache.is_valid(fp)

        cache = self._entries[key]
        if cache is None:
            return False

        if not os.path.exists(fp):
            return False

        if os.path.getmtime(fp) > cache["last_modified"]:
            return False

        return True
