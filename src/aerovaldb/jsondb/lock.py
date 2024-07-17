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

    async def acquire(
        self,
        blocking: bool = True,
        delay: float = 0.01,
        max_delay: float = 0.1,
        timeout: float | None = None,
    ) -> bool:
        logger.debug("Acquiring lock with lockfile %s", self._lock_file)
        await self._aiolock.acquire()
        success = self._iplock.acquire(
            blocking=blocking, delay=delay, max_delay=max_delay, timeout=timeout
        )

        if not success:
            self._aiolock.release()

        return success

    def release(self):
        logger.debug("Releasing lock with lockfile %s", self._lock_file)
        self._iplock.unlock()
        self._aiolock.release()

    def is_locked(self) -> bool:
        return self._aiolock.locked() and self._iplock.acquired
