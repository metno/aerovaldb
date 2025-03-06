import datetime
import importlib.metadata
import inspect
import logging
import os
import sqlite3
import sys
from hashlib import md5
from typing import Any, Awaitable, Callable

import simplejson  # type: ignore
from async_lru import alru_cache
from packaging.version import Version

import aerovaldb
from aerovaldb.utils.filter import (
    filter_contour,
    filter_heatmap,
    filter_map,
    filter_regional_stats,
)
from aerovaldb.utils.query import QueryEntry
from aerovaldb.utils.string_mapper import (
    PriorityMapper,
    StringMapper,
    VersionConstraintMapper,
)

from ..aerovaldb import AerovalDB
from ..exceptions import UnsupportedOperation, UnusedArguments
from ..lock import FakeLock, FileLock
from ..routes import *
from ..types import AccessType
from ..utils import (
    async_and_sync,
    build_uri,
    extract_substitutions,
    json_dumps_wrapper,
    parse_uri,
)

if sys.version_info >= (3, 12):
    from typing import override
else:
    from typing_extensions import override

logger = logging.getLogger(__name__)


class AerovalSqliteDB(AerovalDB):
    """
    Allows reading and writing from sqlite3 database files.
    """

    SQLITE_TIMESTAMP_FORMAT = "%Y-%m-%d %H:%M:%S"

    TABLE_COLUMN_NAMES = {
        "glob_stats": extract_substitutions(Route.GLOB_STATS.value),
        "contour": extract_substitutions(Route.CONTOUR.value),
        "contour1": ["project", "experiment", "obsvar", "model", "timestep"],
        "timeseries": extract_substitutions(Route.TIMESERIES.value),
        "timeseries_weekly": extract_substitutions(Route.TIMESERIES_WEEKLY.value),
        "experiments": extract_substitutions(Route.EXPERIMENTS.value),
        "config": extract_substitutions(Route.CONFIG.value),
        "menu": extract_substitutions(Route.MENU.value),
        "statistics": extract_substitutions(Route.STATISTICS.value),
        "ranges": extract_substitutions(Route.RANGES.value),
        "regions": extract_substitutions(Route.REGIONS.value),
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
        "profiles": extract_substitutions(Route.PROFILES.value),
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
        "forecast": extract_substitutions(Route.FORECAST.value),
        "gridded_map": extract_substitutions(Route.GRIDDED_MAP.value),
        "report": extract_substitutions(Route.REPORT.value),
        "reportimages": extract_substitutions(Route.REPORT_IMAGE.value),
        "mapoverlay": extract_substitutions(Route.MAP_OVERLAY.value),
    }

    TABLE_NAME_TO_ROUTE = {
        "glob_stats": Route.GLOB_STATS.value,
        "contour": Route.CONTOUR.value,
        "contour1": Route.CONTOUR_TIMESPLIT.value,
        "timeseries": Route.TIMESERIES.value,
        "timeseries_weekly": Route.TIMESERIES_WEEKLY.value,
        "experiments": Route.EXPERIMENTS.value,
        "config": Route.CONFIG.value,
        "menu": Route.MENU.value,
        "statistics": Route.STATISTICS.value,
        "ranges": Route.RANGES.value,
        "regions": Route.REGIONS.value,
        "models_style0": Route.MODELS_STYLE.value,
        "models_style1": Route.MODELS_STYLE.value,
        "map0": Route.MAP.value,
        "map1": Route.MAP.value,
        "scatter0": Route.SCATTER.value,
        "scatter1": Route.SCATTER.value,
        "profiles": Route.PROFILES.value,
        "heatmap_timeseries0": Route.HEATMAP_TIMESERIES.value,
        "heatmap_timeseries1": Route.HEATMAP_TIMESERIES.value,
        "heatmap_timeseries2": Route.HEATMAP_TIMESERIES.value,
        "forecast": Route.FORECAST.value,
        "gridded_map": Route.GRIDDED_MAP.value,
        "report": Route.REPORT.value,
        "reportimages": Route.REPORT_IMAGE.value,
        "mapoverlay": Route.MAP_OVERLAY.value,
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
                Route.GLOB_STATS.value: "glob_stats",
                Route.REGIONAL_STATS.value: "glob_stats",
                Route.HEATMAP.value: "glob_stats",
                Route.CONTOUR.value: "contour",
                Route.CONTOUR_TIMESPLIT.value: "contour1",
                Route.TIMESERIES.value: "timeseries",
                Route.TIMESERIES_WEEKLY.value: "timeseries_weekly",
                Route.EXPERIMENTS.value: "experiments",
                Route.CONFIG.value: "config",
                Route.MENU.value: "menu",
                Route.STATISTICS.value: "statistics",
                Route.RANGES.value: "ranges",
                Route.REGIONS.value: "regions",
                Route.MODELS_STYLE.value: PriorityMapper(
                    {
                        "models_style0": "{project}/{experiment}",
                        "models_style1": "{project}",
                    }
                ),
                Route.MAP.value: [
                    VersionConstraintMapper(
                        "map0",
                        min_version="0.13.2",
                    ),
                    VersionConstraintMapper(
                        "map1",
                        max_version="0.13.2",
                    ),
                ],
                Route.SCATTER.value: [
                    VersionConstraintMapper(
                        "scatter0",
                        min_version="0.13.2",
                    ),
                    VersionConstraintMapper(
                        "scatter1",
                        max_version="0.13.2",
                    ),
                ],
                Route.PROFILES.value: "profiles",
                Route.HEATMAP_TIMESERIES.value: [
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
                Route.FORECAST.value: "forecast",
                Route.GRIDDED_MAP.value: "gridded_map",
                Route.REPORT.value: "report",
                Route.REPORT_IMAGE.value: "reportimages",
                Route.MAP_OVERLAY: "mapoverlay",
            },
            version_provider=self._get_version,
        )

        self.FILTERS: dict[str, Callable[..., Awaitable[Any]]] = {
            Route.REGIONAL_STATS.value: filter_regional_stats,
            Route.HEATMAP.value: filter_heatmap,
            Route.CONTOUR.value: filter_contour,
            Route.MAP.value: filter_map,
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
                version = Version(importlib.metadata.version("pyaerocom"))
            except importlib.metadata.PackageNotFoundError:
                version = Version("0.0.1")
            finally:
                return version

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
            if table_name in ("reportimages", "mapoverlay"):
                cur.execute(
                    f"""
                    CREATE TABLE IF NOT EXISTS {table_name}(
                        {column_names},
                        ctime TEXT DEFAULT current_timestamp,
                        mtime TEXT DEFAULT current_timestamp,
                        blob BLOB,
                    
                        UNIQUE({column_names})
                    )
                    """
                )
            else:
                cur.execute(
                    f"""
                    CREATE TABLE IF NOT EXISTS {table_name}(
                        {column_names},
                        ctime TEXT DEFAULT current_timestamp,
                        mtime TEXT DEFAULT current_timestamp,
                        json TEXT,

                    UNIQUE({column_names}))
                    """
                )

            cur.execute(
                f"""
                CREATE TRIGGER IF NOT EXISTS update_Timestamp_Trigger_{table_name} AFTER UPDATE On {table_name}
                BEGIN
                   UPDATE {table_name} SET mtime = current_timestamp WHERE rowid = NEW.rowid;
                END;
                """
            )

        self._con.commit()

    def _get_column_list_and_substitution_list(self, kwargs: dict) -> tuple[str, str]:
        keys = list(kwargs.keys())

        columnlist = ", ".join(keys)
        substitutionlist = ", ".join([f":{k}" for k in keys])

        return (columnlist, substitutionlist)

    @override
    async def _get(self, route, route_args, **kwargs):
        cache = kwargs.pop("cache", False)
        default = kwargs.pop("default", None)
        access_type = self._normalize_access_type(kwargs.pop("access_type", None))

        if access_type in [AccessType.FILE_PATH]:
            raise UnsupportedOperation(
                f"sqlitedb does not support access_type FILE_PATH."
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
        if filter_func:
            filter_vars = {
                k: v
                for k, v in (route_args | kwargs).items()
                if k in inspect.signature(filter_func).parameters.keys()
            }
            if not filter_vars:
                filter_func = None

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
                    if k in ("json", "blob", "ctime", "mtime"):
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

            if access_type == AccessType.MTIME:
                dt = datetime.datetime.strptime(
                    fetched["mtime"], AerovalSqliteDB.SQLITE_TIMESTAMP_FORMAT
                )

            if access_type == AccessType.CTIME:
                dt = datetime.datetime.strptime(
                    fetched["ctime"], AerovalSqliteDB.SQLITE_TIMESTAMP_FORMAT
                )

            return dt

        # Filtered.
        if filter_func is not None:
            if access_type == AccessType.MTIME:
                return datetime.datetime.strptime(
                    fetched["mtime"], AerovalSqliteDB.SQLITE_TIMESTAMP_FORMAT
                )
            if access_type == AccessType.CTIME:
                return datetime.datetime.strptime(
                    fetched["ctime"], AerovalSqliteDB.SQLITE_TIMESTAMP_FORMAT
                )
            obj = simplejson.loads(fetched["json"], allow_nan=True)

            obj = filter_func(obj, **filter_vars)
            if access_type == AccessType.OBJ:
                return obj

            if access_type == AccessType.JSON_STR:
                return json_dumps_wrapper(obj)

        raise UnsupportedOperation

    @override
    async def _put(self, obj, route, route_args, **kwargs):
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
            REPLACE INTO {table_name}({columnlist}, json)
            VALUES({substitutionlist}, :json)
            """,
            args,
        )

        self._set_metadata_by_key(
            "last_modified_by", f"aerovaldb_{aerovaldb.__version__}"
        )
        self._con.commit()

    @async_and_sync
    @override
    async def get_by_uri(
        self,
        uri: str | QueryEntry,
        /,
        access_type: str | AccessType = AccessType.OBJ,
        cache: bool = False,
        default=None,
    ):
        if access_type in [AccessType.URI]:
            return uri

        route, route_args, kwargs = parse_uri(uri)

        if route == Route.REPORT_IMAGE.value:
            return await self.get_report_image(
                route_args["project"],
                route_args["experiment"],
                route_args["path"],
                access_type=access_type,
            )

        if route == Route.MAP_OVERLAY.value:
            return await self.get_map_overlay(
                route_args["project"],
                route_args["experiment"],
                route_args["source"],
                route_args["variable"],
                route_args["date"],
                access_type=access_type,
            )

        return await self._get(
            route,
            route_args,
            access_type=access_type,
            cache=cache,
            default=default,
            **kwargs,
        )

    @async_and_sync
    @override
    async def put_by_uri(self, obj, uri: str | QueryEntry):
        route, route_args, kwargs = parse_uri(uri)
        if route == Route.REPORT_IMAGE.value:
            await self.put_report_image(
                obj, route_args["project"], route_args["experiment"], route_args["path"]
            )
            return

        if route == Route.MAP_OVERLAY.value:
            await self.put_map_overlay(
                obj,
                route_args["project"],
                route_args["experiment"],
                route_args["source"],
                route_args["variable"],
                route_args["date"],
            )
            return

        await self._put(obj, route, route_args, **kwargs)

    @async_and_sync
    @override
    async def list_all(self):
        cur = self._con.cursor()
        result = []
        for table_name in self.TABLE_COLUMN_NAMES.keys():
            route = AerovalSqliteDB.TABLE_NAME_TO_ROUTE[table_name]
            cur.execute(
                f"""
                SELECT * FROM {table_name}
                """
            )
            fetched = cur.fetchall()

            for r in fetched:
                arg_names = extract_substitutions(route)

                route_args = {}
                kwargs = {}
                for k in r.keys():
                    if k in ["json", "blob", "ctime", "mtime"]:
                        continue
                    if k in arg_names:
                        route_args[k] = r[k]
                    else:
                        kwargs[k] = r[k]

                uri = build_uri(route, route_args, kwargs)
                result.append(uri)
        return result

    def _get_lock_file(self) -> str:
        os.makedirs(os.path.expanduser("~/.aerovaldb/.lock/"), exist_ok=True)
        lock_file = os.path.join(
            os.environ.get("AVDB_LOCK_DIR", os.path.expanduser("~/.aerovaldb/.lock/")),
            md5(self._dbfile.encode()).hexdigest(),
        )
        return lock_file

    @override
    def lock(self):
        if self._use_real_lock:
            return FileLock(self._get_lock_file())

        return FakeLock()

    @override
    def list_glob_stats(
        self,
        project: str,
        experiment: str,
        /,
        access_type: str | AccessType = AccessType.URI,
    ):
        if access_type != AccessType.URI:
            raise ValueError(
                f"Invalid access_type. Got {access_type}, expected AccessType.URI"
            )

        cur = self._con.cursor()
        cur.execute(
            f"""
            SELECT * FROM glob_stats
            WHERE project=? AND experiment=?
            """,
            (project, experiment),
        )
        fetched = cur.fetchall()

        route = AerovalSqliteDB.TABLE_NAME_TO_ROUTE["glob_stats"]
        result = []
        for r in fetched:
            arg_names = extract_substitutions(route)
            route_args = {}
            kwargs = {}
            for k in r.keys():
                if k in ["json", "blob", "ctime", "mtime"]:
                    continue

                if k in arg_names:
                    route_args[k] = r[k]
                else:
                    kwargs[k] = r[k]

            uri = build_uri(route, route_args, kwargs)
            result.append(uri)

        return result

    @async_and_sync
    @override
    async def list_timeseries(
        self,
        project: str,
        experiment: str,
        /,
        access_type: str | AccessType = AccessType.URI,
    ):
        if access_type != AccessType.URI:
            raise ValueError(
                f"Invalid access_type. Got {access_type}, expected AccessType.URI"
            )

        cur = self._con.cursor()
        cur.execute(
            f"""
            SELECT * FROM timeseries
            WHERE project=? AND experiment=?
            """,
            (project, experiment),
        )
        fetched = cur.fetchall()

        route = AerovalSqliteDB.TABLE_NAME_TO_ROUTE["timeseries"]
        result = []
        for r in fetched:
            arg_names = extract_substitutions(route)
            route_args = {}
            kwargs = {}
            for k in r.keys():
                if k in ["json", "blob", "ctime", "mtime"]:
                    continue

                if k in arg_names:
                    route_args[k] = r[k]
                else:
                    kwargs[k] = r[k]

            uri = build_uri(route, route_args, kwargs)
            result.append(uri)
        return result

    @override
    def rm_experiment_data(self, project: str, experiment: str) -> None:
        cur = self._con.cursor()
        for table in [
            "glob_stats",
            "contour",
            "contour1",
            "timeseries",
            "timeseries_weekly",
            "config",
            "menu",
            "statistics",
            "ranges",
            "regions",
            "models_style0",
            "map0",
            "map1",
            "scatter0",
            "scatter1",
            "profiles",
            "heatmap_timeseries0",
            "heatmap_timeseries1",
            "heatmap_timeseries2",
            "forecast",
            "gridded_map",
            "mapoverlay",
        ]:
            cur.execute(
                f"""
                DELETE FROM {table} WHERE project=? AND experiment=?
                """,
                (project, experiment),
            )

    @async_and_sync
    @override
    async def get_report_image(
        self,
        project: str,
        experiment: str,
        path: str,
        access_type: str | AccessType = AccessType.BLOB,
    ):
        access_type = self._normalize_access_type(access_type)

        if access_type not in [AccessType.BLOB, AccessType.MTIME, AccessType.CTIME]:
            raise UnsupportedOperation(
                f"Sqlitedb does not support accesstype {access_type}."
            )

        cur = self._con.cursor()
        cur.execute(
            """
            SELECT * FROM reportimages
            WHERE
                (project, experiment, path) = (?, ?, ?)
            """,
            (project, experiment, path),
        )
        fetched = cur.fetchone()

        if fetched is None:
            raise FileNotFoundError(f"Object not found. {project, experiment, path}")

        if access_type == AccessType.BLOB:
            return fetched["blob"]

        if access_type == AccessType.MTIME:
            return datetime.datetime.strptime(
                fetched["mtime"], AerovalSqliteDB.SQLITE_TIMESTAMP_FORMAT
            )

        if access_type == AccessType.CTIME:
            return datetime.datetime.strptime(
                fetched["ctime"], AerovalSqliteDB.SQLITE_TIMESTAMP_FORMAT
            )

    @async_and_sync
    @override
    async def put_report_image(self, obj, project: str, experiment: str, path: str):
        cur = self._con.cursor()

        if not isinstance(obj, bytes):
            raise TypeError(f"Expected bytes. Got {type(obj)}")

        cur.execute(
            """
            INSERT OR REPLACE INTO reportimages(project, experiment, path, blob)
            VALUES(?, ?, ?, ?)
            """,
            (project, experiment, path, obj),
        )
        self._con.commit()

    @async_and_sync
    @override
    async def get_map_overlay(
        self,
        project: str,
        experiment: str,
        source: str,
        variable: str,
        date: str,
        access_type: str | AccessType = AccessType.BLOB,
    ):
        access_type = self._normalize_access_type(access_type)

        if access_type not in [AccessType.BLOB, AccessType.MTIME, AccessType.CTIME]:
            raise UnsupportedOperation(
                f"Sqlitedb does not support accesstype {access_type}."
            )

        cur = self._con.cursor()
        cur.execute(
            """
            SELECT * FROM mapoverlay
            WHERE
                (project, experiment, source, variable, date) = (?, ?, ?, ?, ?)
            """,
            (project, experiment, source, variable, date),
        )
        fetched = cur.fetchone()

        if fetched is None:
            raise FileNotFoundError(
                f"Object not found. {project, experiment, source, variable, date}"
            )

        if access_type == AccessType.BLOB:
            return fetched["blob"]

        if access_type == AccessType.MTIME:
            return datetime.datetime.strptime(
                fetched["mtime"], AerovalSqliteDB.SQLITE_TIMESTAMP_FORMAT
            )

        if access_type == AccessType.CTIME:
            return datetime.datetime.strptime(
                fetched["ctime"], AerovalSqliteDB.SQLITE_TIMESTAMP_FORMAT
            )

    @async_and_sync
    @override
    async def put_map_overlay(
        self,
        obj,
        project: str,
        experiment: str,
        source: str,
        variable: str,
        date: str,
    ):
        cur = self._con.cursor()

        if not isinstance(obj, bytes):
            raise TypeError(f"Expected bytes. Got {type(obj)}")

        cur.execute(
            """
            INSERT OR REPLACE INTO mapoverlay(project, experiment, source, variable, date, blob)
            VALUES(?, ?, ?, ?, ?, ?)
            """,
            (project, experiment, source, variable, date, obj),
        )
        self._con.commit()

    @async_and_sync
    @override
    async def get_contour(
        self,
        project: str,
        experiment: str,
        obsvar: str,
        model: str,
        /,
        *args,
        timestep: str,
        access_type: str | AccessType = AccessType.OBJ,
        cache: bool = False,
        default=None,
        **kwargs,
    ):
        access_type = self._normalize_access_type(access_type)
        try:
            result = await self._get(
                Route.CONTOUR.value,
                {
                    "project": project,
                    "experiment": experiment,
                    "obsvar": obsvar,
                    "model": model,
                },
                timestep=timestep,
                access_type=access_type,
                cache=cache,
            )
        except (FileNotFoundError, KeyError):
            pass
        else:
            return result

        try:
            result = await self._get(
                Route.CONTOUR_TIMESPLIT.value,
                {
                    "project": project,
                    "experiment": experiment,
                    "obsvar": obsvar,
                    "model": model,
                    "timestep": timestep,
                },
                access_type=access_type,
                cache=cache,
            )
        except FileNotFoundError:
            pass
        else:
            return result

        if default is not None:
            return default

        raise FileNotFoundError

    @async_and_sync
    @override
    async def put_contour(
        self,
        obj,
        project: str,
        experiment: str,
        obsvar: str,
        model: str,
        /,
        timestep: str,
        *args,
        **kwargs,
    ):
        if timestep is None:
            logger.warning(
                "Writing contours without providing timestep is deprecated and will be removed in a future release."
            )

            await self._put(
                obj,
                Route.CONTOUR.value,
                {
                    "project": project,
                    "experiment": experiment,
                    "obsvar": obsvar,
                    "model": model,
                },
            )
            return

        await self._put(
            obj,
            Route.CONTOUR_TIMESPLIT.value,
            {
                "project": project,
                "experiment": experiment,
                "obsvar": obsvar,
                "model": model,
                "timestep": timestep,
            },
        )
