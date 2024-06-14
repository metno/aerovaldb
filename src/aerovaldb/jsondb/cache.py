from collections import defaultdict
from pathlib import Path
from ..utils import async_and_sync
import logging
import os
import aiofile
import time
from dataclasses import dataclass
from typing import TypedDict

logger = logging.getLogger(__name__)


class CacheEntry(TypedDict):
    json: str

    last_modified: float
    last_accessed: float


class JSONCache:
    """
    Implements an in-memory cache for file access in aerovaldb.
    """

    def __init__(self):
        self.invalidate_all()

    def invalidate_all(self) -> None:
        logger.debug("JSON Cache invalidated.")
        self._cache: defaultdict[str, CacheEntry | None] = defaultdict(lambda: None)

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
            logger.debug(f"Returning contents from file {abspath} from cache.")
            self._cache[abspath]["last_accessed"] = time.time()  # type: ignore
            return self._cache[abspath]["json"]  # type: ignore

        logger.debug(f"Reading file {abspath} and adding to cache.")
        json = await self._read_json(abspath)
        self._cache[abspath] = {
            "json": json,
            "last_modified": os.path.getmtime(abspath),
            "last_accessed": time.time(),
        }
        return json

    def invalidate_cache(self, file_path: str | Path) -> None:
        """
        Invalidates the cache for a file path, ensuring it will be re-read on the next read.

        :param file_path : The file path to invalidate cache for.
        """
        abspath = self._canonical_file_path(file_path)
        logger.debug(f"Invalidating cache for file {abspath}.")
        self._cache[abspath] = None

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

        if os.path.getmtime(abspath) > cache["last_modified"]:
            return False

        return True
