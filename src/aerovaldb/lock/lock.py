import asyncio
import fcntl
import logging
import pathlib
import sys
from abc import ABC, abstractmethod

from ..utils import has_async_loop, run_until_finished

if sys.version_info >= (3, 12):
    from typing import override
else:
    from typing_extensions import override

logger = logging.getLogger(__name__)


class AerovaldbLock(ABC):
    """
    Interface for a context manager based locking mechanism.

    Can be used as a context manager (ie. in a with block).
    """

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_value, exc_traceback):
        self.release()

    @abstractmethod
    def acquire(self):
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


class FakeLock(AerovaldbLock):
    """
    A lock class which does not lock anything. Useful for
    disabling locking when it is not really needed, but leave
    code written for using locking.
    """

    def __init__(self):
        logger.debug("Initializing FAKE lock")
        self.acquire()

    @override
    def acquire(self):
        self._acquired = True

    @override
    def release(self):
        self._acquired = False

    @override
    def is_locked(self) -> bool:
        return self._acquired


class FileLock(AerovaldbLock):
    def __init__(self, lock_file: str | pathlib.Path):
        logger.debug("Initializing lock with lockfile %s", lock_file)
        self._lock_file = lock_file
        self._lock_handle = open(lock_file, "a+")
        self._aiolock = asyncio.Lock()
        self.acquire()

    @override
    def acquire(self):
        logger.debug("Acquiring lock with lockfile %s", self._lock_file)

        if has_async_loop():
            run_until_finished(self._aiolock.acquire)

        fcntl.lockf(self._lock_handle, fcntl.LOCK_EX)
        self._acquired = True

    @override
    def release(self):
        logger.debug("Releasing lock with lockfile %s", self._lock_file)

        fcntl.fcntl(self._lock_handle, fcntl.LOCK_UN)
        self._acquired = False
        if self._aiolock.locked():
            self._aiolock.release()

    @override
    def is_locked(self) -> bool:
        return self._acquired
