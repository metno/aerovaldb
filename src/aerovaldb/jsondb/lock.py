import asyncio
import pathlib
import logging
import fcntl

logger = logging.getLogger(__name__)


class JsonDbLock:
    def __init__(self, lock_file: str | pathlib.Path):
        logger.debug("Initializing lock with lockfile %s", lock_file)
        self._lock_file = lock_file
        self._lock_handle = open(lock_file, "a+")
        self._aiolock = asyncio.Lock()
        self._acquired = False

    async def __aenter__(self):
        await self.acquire()
        return self

    async def __aexit__(self, exc_type, exc_value, exc_traceback):
        self.release()

    async def acquire(self):
        logger.debug("Acquiring lock with lockfile %s", self._lock_file)
        await self._aiolock.acquire()

        fcntl.lockf(self._lock_handle, fcntl.LOCK_EX)
        self._acquired = True

    def release(self):
        logger.debug("Releasing lock with lockfile %s", self._lock_file)

        fcntl.fcntl(self._lock_handle, fcntl.LOCK_UN)
        self._acquired = False
        self._aiolock.release()

    def is_locked(self) -> bool:
        return self._acquired
