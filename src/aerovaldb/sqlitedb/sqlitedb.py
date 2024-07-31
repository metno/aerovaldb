import sqlite3
import aerovaldb
from ..aerovaldb import AerovalDB
from ..routes import *
from .utils import extract_substitutions
import os


class AerovalSqliteDB(AerovalDB):
    """
    Allows reading and writing from sqlite3 database files.
    """

    TABLE_NAME_LOOKUP = {
        ROUTE_GLOB_STATS: "glob_stats",
        ROUTE_REG_STATS: "glob_stats",
        ROUTE_HEATMAP: "glob_stats",
        ROUTE_CONTOUR: "contour",
        ROUTE_TIMESERIES: "timeseries",
        ROUTE_TIMESERIES_WEEKLY: "timeseries_weekly",
        ROUTE_EXPERIMENTS: "experiments",
        ROUTE_CONFIG: "config",
        ROUTE_MENU: "menu",
        ROUTE_STATISTICS: "statistics",
        ROUTE_RANGES: "ranges",
        ROUTE_REGIONS: "regions",
        ROUTE_MODELS_STYLE: "models_style",
        ROUTE_MAP: "map",
        ROUTE_SCATTER: "scatter",
        ROUTE_PROFILES: "profiles",
        ROUTE_HEATMAP_TIMESERIES: "heatmap_timeseries",
        ROUTE_FORECAST: "forecast",
        ROUTE_GRIDDED_MAP: "gridded_map",
        ROUTE_REPORT: "report",
    }

    def __init__(self, database: str, /, **kwargs):
        self._dbfile = database

        if not os.path.exists(database):
            self._con = sqlite3.connect(database)
            self._initialize_db()
        else:
            self._con = sqlite3.connect(database)
            if not self._get_metadata_by_key("created_by") == "aerovaldb":
                ValueError(f"Database {database} is not a valid aerovaldb database.")

    def _get_metadata_by_key(self, key: str) -> str:
        """
        Returns the value associated with a key from the metadata
        table.
        """
        cur = self._con.cursor()

        cur.execute(
            """
            SELECT value FROM metadata
            WHERE key = ?
            """,
            (key,),
        )
        return cur.fetchone()[0]

    def _set_metadata_by_key(self, key: str, value: str):
        """ """
        cur = self._con.cursor()

        cur.execute(
            """
            INSERT OR REPLACE INTO metadata(key, value)
            VALUES(?, ?)
            """,
            (key, value),
        )

    def _initialize_db(self):
        """Given an existing sqlite connection or sqlite3 database
        identifier string, initializes the database so it has the
        necessary tables.
        """
        cur = self._con.cursor()

        # Metadata table for information used internally by aerovaldb.
        cur.execute(
            """
            CREATE TABLE metadata(key, value,
            UNIQUE(key))
            """
        )
        self._set_metadata_by_key("created_by", f"aerovaldb_{aerovaldb.__version__}")
        self._set_metadata_by_key(
            "last_modified_by", f"aerovaldb_{aerovaldb.__version__}"
        )

        # Data tables. Currently one table is used per type of asset
        # stored and json blobs are stored in the json column.
        for route, table_name in self.TABLE_NAME_LOOKUP.items():
            route_args = extract_substitutions(route)

            column_names = ",".join(route_args)
            cur.execute(
                f"""
                CREATE TABLE IF NOT EXISTS {table_name}({column_names},json,

                UNIQUE({column_names}))
                """
            )

    async def _get(self, route, route_args, *args, **kwargs):
        pass

    async def _put(self, obj, route, route_args, *args, **kwargs):
        pass
