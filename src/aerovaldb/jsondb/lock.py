import fasteners  # type: ignore
import asyncio
import pathlib
import logging

logger = logging.getLogger(__name__)


class JsonDbLock:
    def __init__(self, lock_file: str | pathlib.Path):
        logger.debug("Initializing lock with lockfile %s", lock_file)
        self._lock_file = lock_file
        self._aiolock = asyncio.Lock()
        pathlib.Path(lock_file).touch()
        self._iplock = fasteners.InterProcessLock(lock_file)

    async def acquire(self):
        logger.debug("Acquiring lock with lockfile %s", self._lock_file)
        await self._aiolock.acquire()
        self._iplock.acquire()

    def release(self):
        logger.debug("Releasing lock with lockfile %s", self._lock_file)
        self._iplock.unlock()
        self._aiolock.release()

    def has_lock(self) -> bool:
        return self._aiolock.locked() and self._iplock.acquired
