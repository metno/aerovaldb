import datetime
import glob
import importlib.metadata
import inspect
import logging
import os
import shutil
import sys
from hashlib import md5
from pathlib import Path
from typing import Any, Awaitable, Callable, Iterable

import filetype  # type: ignore
import simplejson  # type: ignore
from packaging.version import Version

from aerovaldb.aerovaldb import AerovalDB
from aerovaldb.const import IMG_FILE_EXTS
from aerovaldb.types import AccessType

from ..exceptions import UnsupportedOperation
from ..lock import FakeLock, FileLock
from ..routes import *
from ..utils import (
    async_and_sync,
    build_uri,
    extract_substitutions,
    json_dumps_wrapper,
    parse_formatted_string,
    parse_uri,
    str_to_bool,
)
from ..utils.filter import filter_heatmap, filter_map, filter_regional_stats
from ..utils.string_mapper import StringMapper, VersionConstraintMapper
from .cache import CacheMissError, KeyCacheDecorator, LRUFileCache

if sys.version_info >= (3, 12):
    from typing import override
else:
    from typing_extensions import override

from async_lru import alru_cache

from ..utils.encode import DecodedStr, decode_str, encode_str
from ..utils.query import QueryEntry
from .backwards_compatibility import post_process_args

logger = logging.getLogger(__name__)


class _LiteralArg(str):
    """Custom string instance that behaves identical to a regular string.
    It is only used in internal isinstance checks to decide whether to
    apply character encoding to a provided arg when constructing a file
    name. This is in order to allow for args which represent a path to
    contain a '/' character which is normally encoded.
    """

    pass


class AerovalJsonFileDB(AerovalDB):
    # Character mapping used for encoding and decoding string values in file names.
    # Note: Order matters for correct encoding, and % should always be the last entry in this dict.
    FNAME_ENCODE_CHARS = {"/": "%1", "_": "%2", "%": "%0"}

    def __init__(self, basedir: str | Path):
        """
        :param basedir The root directory where aerovaldb will look for files.
        """
        self._use_real_lock = str_to_bool(
            os.environ.get("AVDB_USE_LOCKING", ""), default=False
        )
        logger.info(
            f"Initializing aerovaldb for '{basedir}' with locking {self._use_real_lock}"
        )

        self._cache = KeyCacheDecorator(LRUFileCache(max_size=64), max_size=512)

        self._basedir = os.path.abspath(basedir)

        if not os.path.exists(self._basedir):
            os.makedirs(self._basedir)

        self.PATH_LOOKUP = StringMapper(
            {
                Route.HEATMAP: "./{project}/{experiment}/hm/glob_stats_{frequency}.json",
                Route.GLOB_STATS: "./{project}/{experiment}/hm/glob_stats_{frequency}.json",
                Route.REGIONAL_STATS: "./{project}/{experiment}/hm/glob_stats_{frequency}.json",
                # For MAP_OVERLAY, extension is excluded but it will be appended after the fact.
                Route.MAP_OVERLAY: "./{project}/{experiment}/overlay/{variable}_{source}/{variable}_{source}_{date}",
                Route.CONTOUR: "./{project}/{experiment}/contour/{obsvar}_{model}.geojson",
                Route.CONTOUR_TIMESPLIT: "./{project}/{experiment}/contour/{obsvar}_{model}/{obsvar}_{model}_{timestep}.geojson",
                Route.TIMESERIES_WEEKLY: [
                    VersionConstraintMapper(
                        "./{project}/{experiment}/ts/diurnal/{location}_{network}_{obsvar}_{layer}.json",
                        min_version="0.29.0.dev1",
                    ),
                    VersionConstraintMapper(
                        "./{project}/{experiment}/ts/diurnal/{location}_{network}-{obsvar}_{layer}.json",
                        max_version="0.29.0.dev1",
                    ),
                ],
                Route.TIMESERIES: [
                    VersionConstraintMapper(
                        "./{project}/{experiment}/ts/{location}_{network}_{obsvar}_{layer}.json",
                        min_version="0.29.0.dev1",
                    ),
                    VersionConstraintMapper(
                        "./{project}/{experiment}/ts/{location}_{network}-{obsvar}_{layer}.json",
                        max_version="0.29.0.dev1",
                    ),
                ],
                Route.EXPERIMENTS: "./{project}/experiments.json",
                Route.CONFIG: "./{project}/{experiment}/cfg_{project}_{experiment}.json",
                Route.MENU: "./{project}/{experiment}/menu.json",
                Route.STATISTICS: "./{project}/{experiment}/statistics.json",
                Route.RANGES: "./{project}/{experiment}/ranges.json",
                Route.REGIONS: "./{project}/{experiment}/regions.json",
                Route.MODELS_STYLE: [
                    "./{project}/{experiment}/models-style.json",
                    "./{project}/models-style.json",
                ],
                Route.MAP: [
                    VersionConstraintMapper(
                        "./{project}/{experiment}/map/{network}_{obsvar}_{layer}_{model}_{modvar}_{time}.json",
                        min_version="0.29.0.dev1",
                    ),
                    VersionConstraintMapper(
                        "./{project}/{experiment}/map/{network}-{obsvar}_{layer}_{model}-{modvar}_{time}.json",
                        min_version="0.13.2",
                        max_version="0.29.0.dev1",
                    ),
                    VersionConstraintMapper(
                        "./{project}/{experiment}/map/{network}-{obsvar}_{layer}_{model}-{modvar}.json",
                        max_version="0.13.2",
                    ),
                ],
                Route.SCATTER: [
                    VersionConstraintMapper(
                        "./{project}/{experiment}/scat/{network}_{obsvar}_{layer}_{model}_{modvar}_{time}.json",
                        min_version="0.29.0.dev1",
                    ),
                    VersionConstraintMapper(
                        "./{project}/{experiment}/scat/{network}-{obsvar}_{layer}_{model}-{modvar}_{time}.json",
                        min_version="0.13.2",
                        max_version="0.29.0.dev1",
                    ),
                    VersionConstraintMapper(
                        "./{project}/{experiment}/scat/{network}-{obsvar}_{layer}_{model}-{modvar}.json",
                        max_version="0.13.2",
                    ),
                ],
                Route.PROFILES: "./{project}/{experiment}/profiles/{location}_{network}_{obsvar}.json",
                Route.HEATMAP_TIMESERIES: [
                    VersionConstraintMapper(
                        "./{project}/{experiment}/hm/ts/{region}_{network}_{obsvar}_{layer}.json",
                        min_version="0.29.0.dev1",
                    ),
                    VersionConstraintMapper(
                        "./{project}/{experiment}/hm/ts/{region}-{network}-{obsvar}-{layer}.json",
                        min_version="0.13.2",  # https://github.com/metno/pyaerocom/blob/4478b4eafb96f0ca9fd722be378c9711ae10c1f6/setup.cfg
                        max_version="0.29.0.dev1",
                    ),
                    VersionConstraintMapper(
                        "./{project}/{experiment}/hm/ts/{network}-{obsvar}-{layer}.json",
                        min_version="0.12.2",
                        max_version="0.13.2",
                    ),
                    VersionConstraintMapper(
                        "./{project}/{experiment}/hm/ts/stats_ts.json",
                        max_version="0.12.2",
                    ),
                ],
                Route.FORECAST: [
                    VersionConstraintMapper(
                        "./{project}/{experiment}/forecast/{region}_{network}_{obsvar}_{layer}.json",
                        min_version="0.29.0.dev1",
                    ),
                    VersionConstraintMapper(
                        "./{project}/{experiment}/forecast/{region}_{network}-{obsvar}_{layer}.json",
                        max_version="0.29.0.dev1",
                    ),
                ],
                Route.FAIRMODE: "./{project}/{experiment}/fairmode/{region}_{network}_{obsvar}_{layer}_{model}_{time}.json",
                Route.GRIDDED_MAP: "./{project}/{experiment}/contour/{obsvar}_{model}.json",
                Route.REPORT: "./reports/{project}/{experiment}/{title}.json",
                Route.REPORT_IMAGE: "./reports/{project}/{experiment}/{path}",
            },
            version_provider=self._get_version,
        )

        self.FILTERS: dict[Route, Callable[..., Awaitable[Any]]] = {
            Route.REGIONAL_STATS: filter_regional_stats,
            Route.HEATMAP: filter_heatmap,
            Route.MAP: filter_map,
        }

    def _load_json(
        self,
        key,
        *,
        access_type: AccessType = AccessType.OBJ,
        cache: bool = False,
    ):
        if access_type in [
            AccessType.BLOB,
            AccessType.URI,
            AccessType.FILE_PATH,
            AccessType.MTIME,
            AccessType.CTIME,
        ]:
            ValueError(f"Unable to load json with access_type={access_type}.")

        json_str = self._cache.get(key, bypass_cache=not cache)
        if access_type == AccessType.JSON_STR:
            return json_str

        elif access_type == AccessType.OBJ:
            return simplejson.loads(json_str, allow_nan=True)

        raise UnsupportedOperation(f"{access_type}")

    @async_and_sync
    @alru_cache(maxsize=2048)
    async def _get_version(
        self, project: DecodedStr, experiment: DecodedStr
    ) -> Version:
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

    @async_and_sync
    async def _get_template(
        self, route: Route, substitutions: dict[str, DecodedStr]
    ) -> str:
        """
        Loops through each instance of TemplateMapper finding the
        appropriate template to use give an route, and a dictionary
        of substitutions.

        :param route : The route for which to look up the template.
        :param substitutions : Dictionary of format substitutions available.

        :returns The template string.

        :raises TemplateNotFound :
            If no valid template was found.
        """
        return await self.PATH_LOOKUP.lookup(route, **substitutions)

    def _prepare_substitutions(self, subs: dict[str, _LiteralArg | str]) -> dict:
        """Prepares template substitutions for inclusion in file path. This mainly
        entails file name encoding.

        :param subs: Dict of subs to be prepared.

        :return: Dict with same keys with the prepared values.
        """
        return {
            k: v
            if isinstance(v, _LiteralArg)
            else encode_str(v, encode_chars=AerovalJsonFileDB.FNAME_ENCODE_CHARS)
            for k, v in subs.items()
        }

    @override
    async def _get(
        self,
        route,
        route_args,
        **kwargs,
    ):
        use_caching = kwargs.pop("cache", False)
        default = kwargs.pop("default", None)

        _try_unencoded = kwargs.pop("_try_unencoded", True)
        _raise_file_not_found_error = kwargs.pop("_raise_file_not_found_error", True)
        access_type = self._normalize_access_type(kwargs.pop("access_type", None))

        path_template = await self._get_template(route, (route_args | kwargs))
        logger.debug(f"Using template string {path_template}")

        substitutions = self._prepare_substitutions(route_args | kwargs)

        logger.debug(f"Fetching data for {route}.")

        relative_path = path_template.format(**substitutions)

        file_path = os.path.join(self._basedir, relative_path)

        if _try_unencoded and not os.path.exists(file_path):
            file_path = os.path.join(
                self._basedir, path_template.format(**(route_args | kwargs))
            )

        logger.debug(f"Fetching file {file_path} as {access_type}-")

        filter_func = self.FILTERS.get(route, None)
        if filter_func:
            filter_vars = {
                k: v
                for k, v in (route_args | kwargs).items()
                if k in inspect.signature(filter_func).parameters.keys()
            }
            if not filter_vars:
                filter_func = None

        if not os.path.exists(file_path):
            if default is None or access_type == AccessType.FILE_PATH:
                if _raise_file_not_found_error:
                    raise FileNotFoundError(f"File {file_path} does not exist.")
                else:
                    return file_path
            return default

        if filter_func is None:
            if access_type == AccessType.FILE_PATH:
                return file_path
            if access_type == AccessType.URI:
                return build_uri(route, route_args, kwargs)

            if access_type == AccessType.MTIME:
                return datetime.datetime.fromtimestamp(os.path.getmtime(file_path))
            if access_type == AccessType.CTIME:
                return datetime.datetime.fromtimestamp(os.path.getctime(file_path))

            return self._load_json(
                file_path, access_type=access_type, cache=use_caching
            )

        if access_type == AccessType.FILE_PATH:
            raise UnsupportedOperation("Filtered endpoints can not return a filepath")

        if access_type == AccessType.MTIME:
            return datetime.datetime.fromtimestamp(os.path.getmtime(file_path))
        if access_type == AccessType.CTIME:
            return datetime.datetime.fromtimestamp(os.path.getctime(file_path))

        filter_params = [kwargs[k] for k in sorted(kwargs.keys())]
        key = f"{file_path}::{'/'.join(filter_params)}"

        try:
            obj = self._load_json(key, access_type=AccessType.OBJ, cache=use_caching)
        except CacheMissError:
            obj = self._load_json(file_path)
            obj = filter_func(obj, **filter_vars)
            if use_caching:
                self._cache.put(json_dumps_wrapper(obj), key=key)

        if access_type == AccessType.OBJ:
            return obj

        if access_type == AccessType.JSON_STR:
            return json_dumps_wrapper(obj)

        raise UnsupportedOperation

    @override
    async def _put(self, obj, route, route_args, **kwargs):
        """Jsondb implemention of database put operation.

        If obj is string, it is assumed to be a wellformatted json string.
        Otherwise it is assumed to be a serializable python object.
        """
        path_template = await self._get_template(route, route_args | kwargs)

        assert all(
            isinstance(v, DecodedStr | str) for v in (route_args | kwargs).values()
        )
        substitutions = self._prepare_substitutions(route_args | kwargs)

        relative_path = path_template.format(**substitutions)

        file_path = str(Path(os.path.join(self._basedir, relative_path)).resolve())

        logger.debug(f"Mapped route {route} / { route_args} to file {file_path}.")

        if not os.path.exists(os.path.dirname(file_path)):
            os.makedirs(os.path.dirname(file_path))
        if isinstance(obj, str):
            json = obj
        else:
            json = json_dumps_wrapper(obj)
        with open(file_path, "w") as f:
            f.write(json)

    @override
    def rm_experiment_data(self, project: str, experiment: str) -> None:
        """Deletes ALL data associated with an experiment.

        :param project : Project ID.
        :param experiment : Experiment ID.
        """
        exp_dir = os.path.join(self._basedir, project, experiment)

        if os.path.exists(exp_dir):
            logger.info(
                f"Removing experiment data for project {project}, experiment, {experiment}."
            )
            shutil.rmtree(exp_dir)

    @async_and_sync
    @override
    async def get_regional_stats(
        self,
        project: str,
        experiment: str,
        frequency: str,
        network: str,
        variable: str,
        layer: str,
        /,
        *args,
        **kwargs,
    ):
        """Fetches regional stats from the database.

        :param project: The project ID.
        :param experiment: The experiment ID.
        :param frequency: The frequency.
        :param network: Observation network.
        :param variable: Variable name.
        """
        return await self._get(
            Route.REGIONAL_STATS,
            {"project": project, "experiment": experiment, "frequency": frequency},
            access_type=kwargs.get("access_type", AccessType.OBJ),
            network=network,
            variable=variable,
            layer=layer,
            cache=True,
        )

    @async_and_sync
    @override
    async def get_heatmap(
        self,
        project: str,
        experiment: str,
        frequency: str,
        region: str,
        time: str,
        /,
        *args,
        **kwargs,
    ):
        """Fetches heatmap data from the database

        :param project: The project ID.
        :param experiment: The experiment ID.
        :param frequency: The frequency.
        :param region: Region.
        :param time: Time.
        """
        return await self._get(
            Route.HEATMAP,
            {"project": project, "experiment": experiment, "frequency": frequency},
            access_type=kwargs.get("access_type", AccessType.OBJ),
            region=region,
            time=time,
            cache=True,
        )

    @async_and_sync
    @alru_cache(maxsize=1000)
    async def _get_query_entry_for_file(self, file_path: str) -> QueryEntry:
        """
        For the provided data file path, returns the corresponding
        URI.

        :param file_path : The file_path.
        """
        file_path = os.path.join(self._basedir, file_path)
        file_path = os.path.relpath(file_path, start=self._basedir)

        _, ext = os.path.splitext(file_path)

        if "/overlay/" in file_path:
            file_path = str(Path(file_path).parent / Path(file_path).stem)

        if file_path.startswith("reports/") and ext.lower() in IMG_FILE_EXTS:
            # TODO: Fix this.
            # The image endpoint is the only endpoint which needs to accept an arbitrary path
            # under the experiment directory. Treating it as a special case for now.
            split = file_path.split("/")
            project = split[1]
            experiment = split[2]
            path = "/".join(split[3:])
            uri = build_uri(
                Route.REPORT_IMAGE,
                {
                    "project": decode_str(
                        project, encode_chars=AerovalJsonFileDB.FNAME_ENCODE_CHARS
                    ),
                    "experiment": decode_str(
                        experiment, encode_chars=AerovalJsonFileDB.FNAME_ENCODE_CHARS
                    ),
                    "path": path,
                },
                {},
            )
            entry = QueryEntry(
                uri,
                Route.REPORT_IMAGE,
                {"project": project, "experiment": experiment, "path": path},
            )
            return entry

        for route in self.PATH_LOOKUP._lookuptable:
            if not (route == Route.MODELS_STYLE):
                if file_path.startswith("reports/"):
                    _str = "/".join(file_path.split("/")[1:3])
                    subs = parse_formatted_string("{project}/{experiment}", _str)
                else:
                    _str = "/".join(file_path.split("/")[0:2])
                    subs = parse_formatted_string("{project}/{experiment}", _str)
            else:
                try:
                    subs = parse_formatted_string(
                        "{project}/{experiment}/models-style.json", file_path
                    )
                except Exception:
                    try:
                        subs = parse_formatted_string(
                            "{project}/models-style.json", file_path
                        )
                    except:
                        continue

            subs = {
                k: decode_str(v, encode_chars=AerovalJsonFileDB.FNAME_ENCODE_CHARS)
                for k, v in subs.items()
            }
            template = await self._get_template(route, subs)

            if "experiment" in subs:
                version = await self._get_version(subs["project"], subs["experiment"])
            else:
                # Project level models style does not have a version because version is defined
                # per experiment. version doesn't matter for models-style because it is priority
                # based, so we set a dummy value to simplify.
                version = Version("0.0.1")
            route_arg_names = extract_substitutions(route.value)

            try:
                all_args = parse_formatted_string(template, f"./{file_path}")  # type: ignore
                route_args = {k: v for k, v in all_args.items() if k in route_arg_names}
                kwargs = {
                    k: v for k, v in all_args.items() if not (k in route_arg_names)
                }
                route_args, kwargs = post_process_args(
                    route, route_args, kwargs, version=version
                )
                route_args = {
                    k: decode_str(v, encode_chars=AerovalJsonFileDB.FNAME_ENCODE_CHARS)
                    for k, v in route_args.items()
                }
                kwargs = {
                    k: decode_str(v, encode_chars=AerovalJsonFileDB.FNAME_ENCODE_CHARS)
                    for k, v in kwargs.items()
                }
            except Exception:
                continue
            else:
                uri = build_uri(route, route_args, kwargs | {"version": str(version)})
                entry = QueryEntry(uri, Route(route), route_args | kwargs)
                return entry

        raise ValueError(f"Unable to build URI for file path {file_path}")

    @async_and_sync
    @override
    async def list_timeseries(
        self,
        project: str,
        experiment: str,
    ):
        logger.warning("list_all is deprecated. Please consider using query() instead.")
        return [
            uri.uri
            for uri in await self.query(
                Route.TIMESERIES, project=project, experiment=experiment
            )
        ]

    @async_and_sync
    @override
    async def list_map(
        self,
        project: str,
        experiment: str,
    ):
        logger.warning("list_all is deprecated. Please consider using query() instead.")
        return [
            uri.uri
            for uri in await self.query(
                Route.MAP, project=project, experiment=experiment
            )
        ]

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
        access_type = self._normalize_access_type(access_type)
        if access_type in [AccessType.URI]:
            return uri

        route, route_args, kwargs = parse_uri(uri)

        if route.value.startswith("/v0/report-image/"):
            return await self.get_report_image(
                route_args["project"],
                route_args["experiment"],
                route_args["path"],
                access_type=access_type,
            )

        if route.value.startswith("/v0/map-overlay/"):
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
            cache=cache,
            default=default,
            access_type=access_type,
            **kwargs,
        )

    @async_and_sync
    @override
    async def put_by_uri(self, obj, uri: str | QueryEntry):
        route, route_args, kwargs = parse_uri(uri)

        if route.value.startswith("/v0/report-image/"):
            await self.put_report_image(
                obj, route_args["project"], route_args["experiment"], route_args["path"]
            )
            return

        if route.value.startswith("/v0/map-overlay/"):
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

    def _get_lock_file(self) -> str:
        os.makedirs(os.path.expanduser("~/.aerovaldb/lock/"), exist_ok=True)
        lock_file = os.path.join(
            os.environ.get("AVDB_LOCK_DIR", os.path.expanduser("~/.aerovaldb/lock/")),
            md5(self._basedir.encode()).hexdigest(),
        )
        return lock_file

    @override
    def lock(self):
        if self._use_real_lock:
            return FileLock(self._get_lock_file())

        return FakeLock()

    @async_and_sync
    @override
    async def query(
        self, asset_type: Route | Iterable[Route] | None = None, **kwargs
    ) -> list[QueryEntry]:
        if asset_type is None:
            asset_type = set(Route)
        elif isinstance(asset_type, Route):
            asset_type = set([asset_type])
        elif isinstance(asset_type, Iterable):
            asset_type = set(asset_type)
        else:
            raise TypeError(f"Expected Route | Iterable[Route]. Got {type(asset_type)}")

        # NOTE: This is a bit of a hacky hard coded way to minimize the number of files that need to
        # be iterated over.
        if "project" in kwargs and "experiment" in kwargs:
            p = encode_str(
                kwargs["project"], encode_chars=AerovalJsonFileDB.FNAME_ENCODE_CHARS
            )
            e = encode_str(
                kwargs["experiment"], encode_chars=AerovalJsonFileDB.FNAME_ENCODE_CHARS
            )
            glb = glob.iglob(
                os.path.join(glob.escape(f"{self._basedir}/{p}/{e}"), "./**"),
                recursive=True,
            )
        elif "project" in kwargs:
            p = encode_str(
                kwargs["project"], encode_chars=AerovalJsonFileDB.FNAME_ENCODE_CHARS
            )
            glb = glob.iglob(
                os.path.join(glob.escape(f"{self._basedir}/{p}"), "./**"),
                recursive=True,
            )
        else:
            glb = glob.iglob(
                os.path.join(glob.escape(self._basedir), "./**"), recursive=True
            )

        result = []
        for f in glb:
            if os.path.isfile(f):
                try:
                    entry = await self._get_query_entry_for_file(f)
                except (ValueError, KeyError):
                    continue
                else:
                    if entry.type in asset_type:
                        if all(entry.meta[k] == v for k, v in kwargs.items()):
                            result.append(entry)

        return result

    @async_and_sync
    @override
    async def list_all(self):
        logger.warning("list_all is deprecated. Please consider using query() instead.")
        return [uri.uri for uri in await self.query()]

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

        if access_type not in (
            AccessType.FILE_PATH,
            AccessType.BLOB,
            AccessType.MTIME,
            AccessType.CTIME,
        ):
            raise UnsupportedOperation(
                f"The report image endpoint does not support access type {access_type}."
            )

        if access_type in (AccessType.MTIME, AccessType.CTIME):
            return await self._get(
                route=Route.REPORT_IMAGE,
                route_args={
                    "project": project,
                    "experiment": experiment,
                    "path": _LiteralArg(path),
                },
                access_type=access_type,
            )
        file_path = await self._get(
            route=Route.REPORT_IMAGE,
            route_args={
                "project": project,
                "experiment": experiment,
                "path": _LiteralArg(path),
            },
            access_type=AccessType.FILE_PATH,
        )
        logger.debug(f"Fetching image with path '{file_path}'")

        if access_type == AccessType.FILE_PATH:
            return file_path

        with open(file_path, "rb") as f:
            return f.read()

    @async_and_sync
    @override
    async def put_report_image(self, obj, project: str, experiment: str, path: str):
        template = await self._get_template(Route.REPORT_IMAGE, {})

        file_path = os.path.join(
            self._basedir,
            template.format(project=project, experiment=experiment, path=path),
        )
        os.makedirs(Path(file_path).parent, exist_ok=True)
        with open(file_path, "wb") as f:
            f.write(obj)

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

        if access_type not in (
            AccessType.FILE_PATH,
            AccessType.BLOB,
            AccessType.MTIME,
            AccessType.CTIME,
        ):
            raise UnsupportedOperation(
                f"The report image endpoint does not support access type {access_type}."
            )

        for ext in IMG_FILE_EXTS:
            file_path = await self._get(
                route=Route.MAP_OVERLAY,
                route_args={
                    "project": project,
                    "experiment": experiment,
                    "source": source,
                    "variable": variable,
                    "date": date,
                },
                _raise_file_not_found_error=False,
                _try_unencoded=False,
                access_type=AccessType.FILE_PATH,
            )

            file_path += ext
            if os.path.exists(file_path):
                break
        else:
            raise FileNotFoundError(
                f"Overlay for {project}/{experiment}/{source}/{variable}/{date} does not exist."
            )

        logger.debug(f"Fetching image with path '{file_path}'")

        if access_type in [AccessType.MTIME]:
            return datetime.datetime.fromtimestamp(os.path.getmtime(file_path))
        if access_type in [AccessType.CTIME]:
            return datetime.datetime.fromtimestamp(os.path.getctime(file_path))

        if access_type == AccessType.FILE_PATH:
            return file_path

        with open(file_path, "rb") as f:
            return f.read()

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
        """Putter for map overlay images.

        :param obj : The object to be stored.
        :param project : Project ID.
        :param experiment : Experiment ID.
        :param source : Data source. Can be either an observation network or a model ID.
        :param variable : Variable name.
        :param date : Date.
        """
        template = await self._get_template(Route.MAP_OVERLAY, {})

        subs = self._prepare_substitutions(
            {
                "project": project,
                "experiment": experiment,
                "source": source,
                "variable": variable,
                "date": date,
            }
        )
        file_path = os.path.join(
            self._basedir,
            template.format(**subs),
        )

        ext = filetype.guess_extension(obj)
        if ext is None:
            raise ValueError(
                f"Could not guess image file extension of provided image data starting with '0x{obj[:20].hex()}'."
            )
        file_path += f".{ext}"

        Path(file_path).parent.mkdir(exist_ok=True, parents=True)
        with open(file_path, "wb") as f:
            f.write(obj)

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
            file_path = await self._get(
                Route.CONTOUR,
                {
                    "project": project,
                    "experiment": experiment,
                    "obsvar": obsvar,
                    "model": model,
                },
                # timestep=timestep,
                access_type=AccessType.FILE_PATH,
                cache=True,
            )
            try:
                if timestep is None:
                    key = file_path
                else:
                    key = f"{file_path}::{timestep}"

                result = simplejson.loads(self._cache.get(key), allow_nan=True)
            except CacheMissError:
                result = await self._get(
                    Route.CONTOUR,
                    {
                        "project": project,
                        "experiment": experiment,
                        "obsvar": obsvar,
                        "model": model,
                    },
                    access_type=AccessType.OBJ,
                    cache=True,
                )
                if timestep is not None:
                    for t, value in result.items():
                        self._cache.put(
                            json_dumps_wrapper(value), key=f"{file_path}::{t}"
                        )
                    result = result[timestep]
        except (FileNotFoundError, KeyError):
            pass
        else:
            if access_type == AccessType.OBJ:
                return result
            if access_type == AccessType.JSON_STR:
                return json_dumps_wrapper(result)

        try:
            result = await self._get(
                Route.CONTOUR_TIMESPLIT,
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
        timestep: str | None = None,
        *args,
        **kwargs,
    ):
        if timestep is None:
            logger.warning(
                "Writing contours without providing timestep is deprecated and will be removed in a future release."
            )

            await self._put(
                obj,
                Route.CONTOUR,
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
            Route.CONTOUR_TIMESPLIT,
            {
                "project": project,
                "experiment": experiment,
                "obsvar": obsvar,
                "model": model,
                "timestep": timestep,
            },
        )

    @async_and_sync
    @override
    async def rm_by_uri(self, uri: str | QueryEntry):
        file_path = await self.get_by_uri(str(uri), access_type=AccessType.FILE_PATH)
        logger.debug("Removing file '%s'.", file_path)

        if os.path.exists(file_path):
            os.remove(file_path)

    @async_and_sync
    @override
    async def list_glob_stats(
        self,
        project: str,
        experiment: str,
        /,
        access_type: str | AccessType = AccessType.URI,
    ) -> list[str]:
        logger.warning("list_glob_stats() is deprecated. Please use query instead.")

        # Route.HEATMAP below is intentional, as this maintains old behaviour for compatibility.
        # Will be removed in future since the name is misleading (it returns heatmap while being
        # named list_glob_stats).
        return [
            uri.uri
            for uri in await self.query(
                Route.HEATMAP, project=project, experiment=experiment
            )
        ]

    @async_and_sync
    @override
    async def get_report(
        self,
        project: str,
        experiment: str,
        title: str,
        /,
        *args,
        access_type: str | AccessType = AccessType.OBJ,
        cache: bool = False,
        default=None,
        **kwargs,
    ):
        return await self._get(
            Route.REPORT,
            {
                "project": _LiteralArg(project),
                "experiment": _LiteralArg(experiment),
                "title": _LiteralArg(title),
            },
            access_type=access_type,
            cache=cache,
            default=default,
        )

    @async_and_sync
    @override
    async def put_report(
        self, obj, project: str, experiment: str, title: str, /, *args, **kwargs
    ):
        """Store report.

        :param obj: The object to be stored.
        :param project: Project ID.
        :param experiment: Experiment ID.
        :param title: Report title (ie. filename without extension).
        """
        await self._put(
            obj,
            Route.REPORT,
            {
                "project": _LiteralArg(project),
                "experiment": _LiteralArg(experiment),
                "title": _LiteralArg(title),
            },
            **kwargs,
        )

    @async_and_sync
    @override
    async def get_config(
        self,
        project: str,
        experiment: str,
        /,
        *args,
        access_type: str | AccessType = AccessType.OBJ,
        cache: bool = False,
        default=None,
        **kwargs,
    ):
        data = None
        try:
            data = await self._get(
                Route.CONFIG,
                {
                    "project": _LiteralArg(project),
                    "experiment": _LiteralArg(experiment),
                },
                access_type=access_type,
                cache=cache,
            )
        except FileNotFoundError:
            data = await self._get(
                Route.CONFIG,
                {"project": project, "experiment": experiment},
                access_type=access_type,
                cache=cache,
            )

        if data is None and default is not None:
            data = default

        if data is not None:
            return data

        raise FileNotFoundError

    @async_and_sync
    @override
    async def put_config(self, obj, project: str, experiment: str, /, *args, **kwargs):
        await self._put(
            obj,
            Route.CONFIG,
            {
                "project": _LiteralArg(project),
                "experiment": _LiteralArg(experiment),
            },
            **kwargs,
        )
