import asyncio
import pathlib
import logging
import fcntl

logger = logging.getLogger(__name__)


class JsonDbLock:
    def __init__(self, lock_file: str | pathlib.Path, uuid: str):
        self._uuid = uuid
        logger.debug("Initializing lock with lockfile %s", lock_file)
        self._lock_file = open(lock_file, "w")
        self._aiolock = asyncio.Lock()

    async def __aenter__(self):
        await self.acquire()
        return self

    async def __aexit__(self, exc_type, exc_value, exc_traceback):
        self.release()

    async def acquire(self):
        logger.debug("Acquiring lock with lockfile %s", self._lock_file)
        await self._aiolock.acquire()

        fcntl.lockf(self._lock_file, fcntl.LOCK_EX)

    def release(self):
        logger.debug("Releasing lock with lockfile %s", self._lock_file)

        fcntl.fcntl(self._lock_file, fcntl.LOCK_UN)
        self._aiolock.release()
