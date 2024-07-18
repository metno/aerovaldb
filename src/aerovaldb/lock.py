from abc import ABC, abstractmethod


class AerovaldbLock(ABC):
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
