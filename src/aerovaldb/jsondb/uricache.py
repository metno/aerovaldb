import sqlite3
import os
from pathlib import Path
from functools import lru_cache
import logging

logger = logging.getLogger(__name__)


class URICache:
    """
    Cache implementation for URIs. Deriving the URI from file paths
    is a slow operation, so caching these makes sense. This class
    implements a cache for uris with persistent storage in an sqlite
    database.
    """

    def __init__(self, path: str | Path):
        self._initializedb(path)

    def _initializedb(self, path: str | Path):
        path = str(path)
        logger.debug(f"Path: {path}")
        self._con = sqlite3.connect(path)

        cur = self._con.cursor()
        cur.execute(
            """CREATE TABLE IF NOT EXISTS uris(
                uri TEXT,
                file TEXT,
                UNIQUE(uri),
                UNIQUE(file)
            )
            """
        )

        cur.execute(
            """
            CREATE UNIQUE INDEX IF NOT EXISTS uri_index
            ON uris(uri)
            """
        )

        cur.execute(
            """
            CREATE UNIQUE INDEX IF NOT EXISTS file_index
            ON uris(file)
            """
        )

        self._con.commit()

    def remove_entry(self, /, uri: str):
        cur = self._con.cursor()

        cur.execute(
            """
            DELETE FROM uris
            WHERE uri = ?
            """,
            uri,
        )
        self._con.commit()

    def add_entry(self, /, uri: str, file: str):
        cur = self._con.cursor()

        cur.execute(
            """
            INSERT OR IGNORE INTO uris(uri, file)
            VALUES(?, ?)
            """,
            (uri, file),
        )
        self._con.commit()

    @lru_cache
    def get_uri_from_file(self, file: str) -> str | None:
        cur = self._con.cursor()

        cur.execute(
            """
            SELECT uri FROM uris
            WHERE file = ?
            """,
            (file,),
        )

        if (fetched := cur.fetchone()) is not None:
            return fetched[0]

        return None

    @lru_cache
    def get_file_from_uri(self, uri: str) -> str | None:
        cur = self._con.cursor()

        cur.execute(
            """
            SELECT file FROM uris
            WHERE uri = ?
            """,
            (uri,),
        )

        if (fetched := cur.fetchone()) is not None:
            return fetched[0]

        return None
