import logging
import sqlite3
from .aerovaldb import AerovalDB

logger = logging.getLogger(__name__)


class AerovalSqliteDB(AerovalDB):
    def __init__(self, sqlite_db: sqlite3.Connection | str):
        if isinstance(sqlite_db, str):
            self._con = sqlite3.connect(sqlite_db)
        else:
            if isinstance(sqlite_db, sqlite3.Connection):
                self._con = sqlite_db
            else:
                raise ValueError(f"Unable to connect to database {sqlite_db}")

        self._initialize_db()

    def _initialize_db(self):
        """
        Initializes the db, creating any required tables that don't
        currently exist.
        """
        pass

    async def _get(self, route, route_args, *args, **kwargs):
        pass

    def _put(self, obj, route, route_args, *args, **kwargs):
        pass
