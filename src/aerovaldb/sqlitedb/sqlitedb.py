import sqlite3
from typing import Any, Awaitable, Callable

from async_lru import alru_cache
from pkg_resources import DistributionNotFound, get_distribution  # type: ignore
import simplejson  # type: ignore
import aerovaldb
from aerovaldb.utils.filter import filter_heatmap, filter_regional_stats
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
    validate_filename_component,
)
from aerovaldb.utils.string_mapper import (
    StringMapper,
    VersionConstraintMapper,
    PriorityMapper,
)
import os
from ..lock import FakeLock, FileLock
from hashlib import md5
from packaging.version import Version


class AerovalSqliteDB(AerovalDB):
    """
    Allows reading and writing from sqlite3 database files.
    """

    TABLE_COLUMN_NAMES = {
        "glob_stats": extract_substitutions(ROUTE_GLOB_STATS),
        "contour": extract_substitutions(ROUTE_CONTOUR),
        "timeseries": extract_substitutions(ROUTE_TIMESERIES),
        "timeseries_weekly": extract_substitutions(ROUTE_TIMESERIES_WEEKLY),
        "experiments": extract_substitutions(ROUTE_EXPERIMENTS),
        "config": extract_substitutions(ROUTE_CONFIG),
        "menu": extract_substitutions(ROUTE_MENU),
        "statistics": extract_substitutions(ROUTE_STATISTICS),
        "ranges": extract_substitutions(ROUTE_RANGES),
        "regions": extract_substitutions(ROUTE_REGIONS),
        "models_style0": ["project", "experiment"],
        "models_style1": ["project"],
        "map0": [
            "project",
            "experiment",
            "network",
            "obsvar",
            "layer",
            "model",
            "modvar",
            "time",
        ],
        "map1": [
            "project",
            "experiment",
            "network",
            "obsvar",
            "layer",
            "model",
            "modvar",
        ],
        "scatter0": [
            "project",
            "experiment",
            "network",
            "obsvar",
            "layer",
            "model",
            "modvar",
            "time",
        ],
        "scatter1": [
            "project",
            "experiment",
            "network",
            "obsvar",
            "layer",
            "model",
            "modvar",
        ],
        "profiles": extract_substitutions(ROUTE_PROFILES),
        "heatmap_timeseries0": [
            "project",
            "experiment",
            "region",
            "network",
            "obsvar",
            "layer",
        ],
        "heatmap_timeseries1": ["project", "experiment", "network", "obsvar", "layer"],
        "heatmap_timeseries2": [
            "project",
            "experiment",
        ],
        "forecast": extract_substitutions(ROUTE_FORECAST),
        "gridded_map": extract_substitutions(ROUTE_GRIDDED_MAP),
        "report": extract_substitutions(ROUTE_REPORT),
    }

    TABLE_NAME_TO_ROUTE = {
        "glob_stats": ROUTE_GLOB_STATS,
        "contour": ROUTE_CONTOUR,
        "timeseries": ROUTE_TIMESERIES,
        "timeseries_weekly": ROUTE_TIMESERIES_WEEKLY,
        "experiments": ROUTE_EXPERIMENTS,
        "config": ROUTE_CONFIG,
        "menu": ROUTE_MENU,
        "statistics": ROUTE_STATISTICS,
        "ranges": ROUTE_RANGES,
        "regions": ROUTE_REGIONS,
        "models_style0": ROUTE_MODELS_STYLE,
        "models_style1": ROUTE_MODELS_STYLE,
        "map0": ROUTE_MAP,
        "map1": ROUTE_MAP,
        "scatter0": ROUTE_SCATTER,
        "scatter1": ROUTE_SCATTER,
        "profiles": ROUTE_PROFILES,
        "heatmap_timeseries0": ROUTE_HEATMAP_TIMESERIES,
        "heatmap_timeseries1": ROUTE_HEATMAP_TIMESERIES,
        "heatmap_timeseries2": ROUTE_HEATMAP_TIMESERIES,
        "forecast": ROUTE_FORECAST,
        "gridded_map": ROUTE_GRIDDED_MAP,
        "report": ROUTE_REPORT,
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

        self.TABLE_NAME_LOOKUP = StringMapper(
            {
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
                ROUTE_MODELS_STYLE: PriorityMapper(
                    {
                        "models_style0": "{project}/{experiment}",
                        "models_style1": "{project}",
                    }
                ),
                ROUTE_MAP: [
                    VersionConstraintMapper(
                        "map0",
                        min_version="0.13.2",
                    ),
                    VersionConstraintMapper(
                        "map1",
                        max_version="0.13.2",
                    ),
                ],
                ROUTE_SCATTER: [
                    VersionConstraintMapper(
                        "scatter0",
                        min_version="0.13.2",
                    ),
                    VersionConstraintMapper(
                        "scatter1",
                        max_version="0.13.2",
                    ),
                ],
                ROUTE_PROFILES: "profiles",
                ROUTE_HEATMAP_TIMESERIES: [
                    VersionConstraintMapper(
                        "heatmap_timeseries0",
                        min_version="0.13.2",  # https://github.com/metno/pyaerocom/blob/4478b4eafb96f0ca9fd722be378c9711ae10c1f6/setup.cfg
                    ),
                    VersionConstraintMapper(
                        "heatmap_timeseries1",
                        min_version="0.12.2",
                        max_version="0.13.2",
                    ),
                    VersionConstraintMapper(
                        "heatmap_timeseries2",
                        max_version="0.12.2",
                    ),
                ],
                ROUTE_FORECAST: "forecast",
                ROUTE_GRIDDED_MAP: "gridded_map",
                ROUTE_REPORT: "report",
            },
            version_provider=self._get_version,
        )

        self.FILTERS: dict[str, Callable[..., Awaitable[Any]]] = {
            ROUTE_REG_STATS: filter_regional_stats,
            ROUTE_HEATMAP: filter_heatmap,
        }

    @async_and_sync
    @alru_cache(maxsize=2048)
    async def _get_version(self, project: str, experiment: str) -> Version:
        """
        Returns the version of pyaerocom used to generate the files for a given project
        and experiment.

        :param project : Project ID.
        :param experiment : Experiment ID.

        :return : A Version object.
        """
        try:
            config = await self.get_config(project, experiment)
        except FileNotFoundError:
            try:
                # If pyaerocom is installed in the current environment, but no config has
                # been written, we use the version of the installed pyaerocom. This is
                # important for tests to work correctly, and for files to be written
                # correctly if the config file happens to be written after data files.
                version = Version(get_distribution("pyaerocom").version)
            except DistributionNotFound:
                version = Version("0.0.1")
            finally:
                return version
        # except simplejson.JSONDecodeError:
        #    # Work around for https://github.com/metno/aerovaldb/issues/28
        #    return Version("0.14.0")

        try:
            version_str = config["exp_info"]["pyaerocom_version"]
            version = Version(version_str)
        except KeyError:
            version = Version("0.0.1")

        return version

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
        for table_name in AerovalSqliteDB.TABLE_COLUMN_NAMES:
            args = AerovalSqliteDB.TABLE_COLUMN_NAMES[table_name]

            column_names = ",".join(args)

            cur.execute(
                f"""
                CREATE TABLE IF NOT EXISTS {table_name}(
                    {column_names},
                    ctime TEXT,
                    mtime TEXT,
                    json TEXT,

                UNIQUE({column_names}))
                """
            )

            cur.execute(
                f"""
                CREATE TRIGGER IF NOT EXISTS insert_Timestamp_Trigger_{table_name}
                AFTER INSERT ON {table_name}
                BEGIN
                   UPDATE {table_name} SET ctime =STRFTIME('%Y-%m-%d %H:%M:%f', 'NOW') WHERE ROWID = NEW.ROWID;
                END;
                """
            )
            cur.execute(
                f"""
                CREATE TRIGGER IF NOT EXISTS update_Timestamp_Trigger_{table_name}
                AFTER UPDATE On {table_name}
                BEGIN
                   UPDATE {table_name} SET mtime = STRFTIME('%Y-%m-%d %H:%M:%f', 'NOW') WHERE ROWID = NEW.ROWID;
                END;
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

        args = route_args | kwargs
        cur = self._con.cursor()
        table_name = await self.TABLE_NAME_LOOKUP.lookup(route, **args)
        args = {
            k: v
            for k, v in args.items()
            if k in AerovalSqliteDB.TABLE_COLUMN_NAMES[table_name]
        }

        [validate_filename_component(x) for x in args.values()]

        columnlist, substitutionlist = self._get_column_list_and_substitution_list(args)
        cur.execute(
            f"""
            SELECT * FROM {table_name}
            WHERE
                ({columnlist}) = ({substitutionlist})
            """,
            args,
        )
        filter_func = self.FILTERS.get(route, None)
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
                    if k in ("json", "ctime", "mtime"):
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

        json = fetched["json"]
        # No filtered.
        if filter_func is None:
            if access_type == AccessType.JSON_STR:
                return json

            if access_type == AccessType.OBJ:
                dt = simplejson.loads(json, allow_nan=True)

            return dt

        # Filtered.
        if filter_func is not None:
            obj = simplejson.loads(fetched["json"], allow_nan=True)

            obj = filter_func(obj, **route_args)
            if access_type == AccessType.OBJ:
                return obj

            if access_type == AccessType.JSON_STR:
                return json_dumps_wrapper(obj)

        raise UnsupportedOperation

    async def _put(self, obj, route, route_args, *args, **kwargs):
        assert len(args) == 0

        cur = self._con.cursor()

        table_name = await self.TABLE_NAME_LOOKUP.lookup(route, **(route_args | kwargs))

        args = route_args | kwargs
        args = {
            k: v
            for k, v in args.items()
            if k in AerovalSqliteDB.TABLE_COLUMN_NAMES[table_name]
        }

        columnlist, substitutionlist = self._get_column_list_and_substitution_list(args)

        json = obj
        if not isinstance(json, str):
            json = json_dumps_wrapper(json)

        args.update(json=json)
        cur.execute(
            f"""
            INSERT OR REPLACE INTO {table_name}({columnlist}, json)
            VALUES({substitutionlist}, :json)
            """,
            args,
        )

        self._set_metadata_by_key(
            "last_modified_by", f"aerovaldb_{aerovaldb.__version__}"
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

        await self._put(obj, route, route_args, **kwargs)

    def list_all(self):
        cur = self._con.cursor()
        for table_name in self.TABLE_COLUMN_NAMES.keys():
            route = AerovalSqliteDB.TABLE_NAME_TO_ROUTE[table_name]
            cur.execute(
                f"""
                SELECT * FROM {table_name}
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
