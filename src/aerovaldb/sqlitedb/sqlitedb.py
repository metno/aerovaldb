import logging
import sqlite3
from ..aerovaldb import AerovalDB
from ..utils import async_and_sync


logger = logging.getLogger(__name__)


class AerovalSqliteDB(AerovalDB):
    SCHEMA_VERSION = "0"

    def __init__(self, sqlite_db: sqlite3.Connection | str):
        if isinstance(sqlite_db, str):
            self._con = sqlite3.connect(sqlite_db)
        else:
            if isinstance(sqlite_db, sqlite3.Connection):
                self._con = sqlite_db
            else:
                raise ValueError(f"Unable to connect to database {sqlite_db}")

        self._cur = self._con.cursor()

        self._initialize_db()

    def _get_schema_version(self) -> int:
        cur = self._cur

        cur.execute(
            """
            SELECT value FROM metadata
            WHERE property=='schema-version' 
            """
        )

        return cur.fetchone()[0]

    def _initialize_db(self):
        """
        Initializes the db, creating any required tables that don't
        currently exist.
        """
        cur = self._cur

        cur.execute(
            """
            CREATE TABLE IF NOT EXISTS metadata (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                property TEXT,
                value TEXT,
                UNIQUE(property)
                )
            """
        )
        cur.execute(
            """
            INSERT OR IGNORE INTO metadata (property, value)
            VALUES(?, ?)
            """,
            ("schema-version", AerovalSqliteDB.SCHEMA_VERSION),
        )
        self._con.commit()

    @async_and_sync
    async def _get(self, route, route_args, *args, **kwargs):
        pass

    @async_and_sync
    def _put(self, obj, route, route_args, *args, **kwargs):
        pass
