import sqlite3

import simplejson  # type: ignore
import aerovaldb
from ..exceptions import UnsupportedOperation, UnusedArguments
from ..aerovaldb import AerovalDB
from ..routes import *
from ..types import AccessType
from ..utils import (
    json_dumps_wrapper,
    parse_uri,
    async_and_sync,
    build_uri,
    extract_substitutions,
)
import os
from ..lock import FakeLock, FileLock
from hashlib import md5


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
        ),
        ROUTE_MODELS_STYLE: ("project", "experiment"),
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
        use_locking = os.environ.get("AVDB_USE_LOCKING", "")
        if use_locking == "0" or use_locking == "":
            self._use_real_lock = False
        else:
            self._use_real_lock = True

        self._dbfile = database

        if not os.path.exists(database):
            self._con = sqlite3.connect(database)
            self._initialize_db()
        else:
            self._con = sqlite3.connect(database)
            if not self._get_metadata_by_key("created_by") == "aerovaldb":
                ValueError(f"Database {database} is not a valid aerovaldb database.")

        self._con.row_factory = sqlite3.Row

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
                CREATE TABLE IF NOT EXISTS {table_name}({column_names},json TEXT,

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
        cache = kwargs.pop("cache", False)
        default = kwargs.pop("default", None)
        if len(args) > 0:
            raise UnusedArguments("Unexpected arguments.")
        access_type = self._normalize_access_type(kwargs.pop("access_type", None))

        if access_type in [AccessType.FILE_PATH]:
            raise UnsupportedOperation(
                f"sqlitedb does not support access_mode FILE_PATH."
            )

        if access_type in [AccessType.URI]:
            return build_uri(route, route_args, kwargs)

        cur = self._con.cursor()

        table_name = AerovalSqliteDB.TABLE_NAME_LOOKUP[route]

        args = route_args | kwargs
        columnlist, substitutionlist = self._get_column_list_and_substitution_list(args)
        cur.execute(
            f"""
            SELECT * FROM {table_name}
            WHERE
                ({columnlist}) = ({substitutionlist})
            """,
            args,
        )
        try:
            fetched = cur.fetchall()
            if not fetched:
                if default is not None:
                    return default
                # For now, raising a FileNotFoundError, since jsondb does and we want
                # them to be interchangeable. Probably should be a aerovaldb custom
                # exception.
                raise FileNotFoundError("Object not found")
            for r in fetched:
                for k in r.keys():
                    if k == "json":
                        continue
                    if not (k in route_args | kwargs) and r[k] is not None:
                        break
                else:
                    fetched = r
                    break

        except TypeError as e:
            # Raising file not found error to be consistent with jsondb implementation.
            # Probably should be a custom exception used by aerovaldb.
            raise FileNotFoundError(
                f"No object found for route, {route}, with args {route_args}, {kwargs}"
            ) from e

        if access_type == AccessType.JSON_STR:
            return fetched["json"]

        if access_type == AccessType.OBJ:
            dt = simplejson.loads(fetched["json"], allow_nan=True)
            return dt

        assert False  # Should never happen.

    async def _put(self, obj, route, route_args, *args, **kwargs):
        assert len(args) == 0

        cur = self._con.cursor()

        table_name = AerovalSqliteDB.TABLE_NAME_LOOKUP[route]

        columnlist, substitutionlist = self._get_column_list_and_substitution_list(
            route_args | kwargs
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
            route_args | kwargs,
        )
        self._con.commit()

    @async_and_sync
    async def get_by_uri(
        self,
        uri: str,
        /,
        access_type: str | AccessType = AccessType.OBJ,
        cache: bool = False,
        default=None,
    ):
        if access_type in [AccessType.URI]:
            return uri

        route, route_args, kwargs = parse_uri(uri)

        return await self._get(
            route,
            route_args,
            access_type=access_type,
            cache=cache,
            default=default,
            **kwargs,
        )

    @async_and_sync
    async def put_by_uri(self, obj, uri: str):
        route, route_args, kwargs = parse_uri(uri)

        # if isinstance(obj, str):
        #    obj = "".join(obj.split(r"\n"))
        await self._put(obj, route, route_args, **kwargs)

    def list_all(self):
        cur = self._con.cursor()
        for route, table in AerovalSqliteDB.TABLE_NAME_LOOKUP.items():
            cur.execute(
                f"""
                SELECT * FROM {table}
                """
            )
            result = cur.fetchall()

            for r in result:
                arg_names = extract_substitutions(route)

                route_args = {}
                kwargs = {}
                for k in r.keys():
                    if k == "json":
                        continue
                    if k in arg_names:
                        route_args[k] = r[k]
                    else:
                        kwargs[k] = r[k]

                # route_args = {k: v for k, v in r.items() if k != "json" and k in arg_names}
                # kwargs = {k: v for k, v in r.items() if k != "json" and not (k in arg_names)}

                route = build_uri(route, route_args, kwargs)
                yield route

    def _get_lock_file(self) -> str:
        os.makedirs(os.path.expanduser("~/.aerovaldb/.lock/"), exist_ok=True)
        lock_file = os.path.join(
            os.environ.get("AVDB_LOCK_DIR", os.path.expanduser("~/.aerovaldb/.lock/")),
            md5(self._dbfile.encode()).hexdigest(),
        )
        return lock_file

    def lock(self):
        if self._use_real_lock:
            return FileLock(self._get_lock_file())

        return FakeLock()
