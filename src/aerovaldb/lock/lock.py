from abc import ABC, abstractmethod
import logging
import fcntl
import asyncio
import pathlib


logger = logging.getLogger(__name__)


class AerovaldbLock(ABC):
    """
    Interface for a context manager based locking mechanism.

    Can be used as a context manager (ie. in a with block).
    """

    async def __aenter__(self):
        await self.acquire()
        return self

    async def __aexit__(self, exc_type, exc_value, exc_traceback):
        self.release()

    @abstractmethod
    async def acquire(self):
        """
        Acquire the lock manually. Usually this should be done
        using a with statement.
        """
        pass

    @abstractmethod
    def release(self):
        """
        Release the lock manually. Usually this should be done
        using a with statement.
        """
        pass

    @abstractmethod
    def is_locked(self) -> bool:
        """
        Check whether the lock is currently acquired.
        """
        pass


class FileLock(AerovaldbLock):
    def __init__(self, lock_file: str | pathlib.Path):
        logger.debug("Initializing lock with lockfile %s", lock_file)
        self._lock_file = lock_file
        self._lock_handle = open(lock_file, "a+")
        self._aiolock = asyncio.Lock()
        self._acquired = False

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
