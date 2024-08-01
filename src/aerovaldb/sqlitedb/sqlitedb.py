import sqlite3

import simplejson  # type: ignore
import aerovaldb
from aerovaldb.exceptions import UnsupportedOperation
from ..aerovaldb import AerovalDB
from ..routes import *
from .utils import extract_substitutions
from ..types import AccessType
from ..utils import json_dumps_wrapper
import os


class AerovalSqliteDB(AerovalDB):
    """
    Allows reading and writing from sqlite3 database files.
    """

    # When creating a table it works to extract the substitution template
    # names from the route, as this constitutes all arguments. For the ones
    # which have extra arguments (currently only time) the following table
    # defines the override. Currently this only applies to map which has
    # an extra time argument.
    ROUTE_COLUMN_NAME_OVERRIDE = {
        ROUTE_MAP: (
            "project",
            "experiment",
            "network",
            "obsvar",
            "layer",
            "model",
            "modvar",
            "time",
        )
    }

    # This lookup table stores the name of the table in which json
    # for a specific route is stored.
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
        for route, table_name in AerovalSqliteDB.TABLE_NAME_LOOKUP.items():
            args = AerovalSqliteDB.ROUTE_COLUMN_NAME_OVERRIDE.get(
                route, extract_substitutions(route)
            )

            column_names = ",".join(args)

            cur.execute(
                f"""
                CREATE TABLE IF NOT EXISTS {table_name}({column_names},json,

                UNIQUE({column_names}))
                """
            )

        self._con.commit()

    def _get_column_list_and_substitution_list(self, kwargs: dict) -> tuple[str, str]:
        keys = list(kwargs.keys())

        columnlist = ", ".join(keys)
        substitutionlist = ", ".join([f":{k}" for k in keys])

        return (columnlist, substitutionlist)

    async def _get(self, route, route_args, *args, **kwargs):
        assert len(args) == 0
        access_type = self._normalize_access_type(kwargs.pop("access_type", None))

        if access_type in [AccessType.FILE_PATH]:
            raise UnsupportedOperation(
                f"sqlitedb does not support access_mode FILE_PATH."
            )

        cur = self._con.cursor()

        table_name = AerovalSqliteDB.TABLE_NAME_LOOKUP[route]

        columnlist, substitutionlist = self._get_column_list_and_substitution_list(
            route_args
        )

        cur.execute(
            f"""
            SELECT json FROM {table_name}
            WHERE
                ({columnlist}) = ({substitutionlist})
            """,
            route_args,
        )
        fetched = cur.fetchone()[0]
        if access_type == AccessType.JSON_STR:
            return fetched

        if access_type == AccessType.OBJ:
            return simplejson.loads(fetched, allow_nan=True)

        assert False  # Should never happen.

    async def _put(self, obj, route, route_args, *args, **kwargs):
        assert len(args) == 0

        cur = self._con.cursor()

        table_name = AerovalSqliteDB.TABLE_NAME_LOOKUP[route]

        columnlist, substitutionlist = self._get_column_list_and_substitution_list(
            route_args
        )

        json = obj
        if not isinstance(json, str):
            json = json_dumps_wrapper(json)

        route_args.update(json=json)
        cur.execute(
            f"""
            INSERT OR REPLACE INTO {table_name}({columnlist}, json)
            VALUES({substitutionlist}, :json)
            """,
            route_args,
        )
