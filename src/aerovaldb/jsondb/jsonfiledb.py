import datetime
import glob
import importlib.metadata
import logging
import os
import shutil
from hashlib import md5
from pathlib import Path
from typing import Any, Awaitable, Callable

import filetype
import simplejson  # type: ignore
from async_lru import alru_cache
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
from ..utils.filter import (
    filter_contour,
    filter_heatmap,
    filter_map,
    filter_regional_stats,
)
from ..utils.string_mapper import StringMapper, VersionConstraintMapper
from .cache import CacheMissError, KeyCacheDecorator, LRUFileCache

logger = logging.getLogger(__name__)


class AerovalJsonFileDB(AerovalDB):
    # Timestep template
    TIMESTEP_TEMPLATE = "{project}/{experiment}/contour/{obsvar}_{model}/{obsvar}_{model}_{timestep}.geojson"

    def __init__(self, basedir: str | Path):
        """
        :param basedir The root directory where aerovaldb will look for files.
        :param asyncio Whether to use asynchronous io to read and store files.
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
                ROUTE_GLOB_STATS: "./{project}/{experiment}/hm/glob_stats_{frequency}.json",
                ROUTE_REG_STATS: "./{project}/{experiment}/hm/glob_stats_{frequency}.json",
                ROUTE_HEATMAP: "./{project}/{experiment}/hm/glob_stats_{frequency}.json",
                # For MAP_OVERLAY, extension is excluded but it will be appended after the fact.
                ROUTE_MAP_OVERLAY: "./{project}/{experiment}/overlay/{variable}_{source}/{variable}_{source}_{date}",
                ROUTE_CONTOUR: "./{project}/{experiment}/contour/{obsvar}_{model}.geojson",
                ROUTE_CONTOUR2: "./{project}/{experiment}/contour/{obsvar}_{model}/{obsvar}_{model}_{timestep}.geojson",
                ROUTE_TIMESERIES_WEEKLY: "./{project}/{experiment}/ts/diurnal/{location}_{network}-{obsvar}_{layer}.json",
                ROUTE_TIMESERIES: "./{project}/{experiment}/ts/{location}_{network}-{obsvar}_{layer}.json",
                ROUTE_EXPERIMENTS: "./{project}/experiments.json",
                ROUTE_CONFIG: "./{project}/{experiment}/cfg_{project}_{experiment}.json",
                ROUTE_MENU: "./{project}/{experiment}/menu.json",
                ROUTE_STATISTICS: "./{project}/{experiment}/statistics.json",
                ROUTE_RANGES: "./{project}/{experiment}/ranges.json",
                ROUTE_REGIONS: "./{project}/{experiment}/regions.json",
                ROUTE_MODELS_STYLE: [
                    "./{project}/{experiment}/models-style.json",
                    "./{project}/models-style.json",
                ],
                ROUTE_MAP: [
                    VersionConstraintMapper(
                        "./{project}/{experiment}/map/{network}-{obsvar}_{layer}_{model}-{modvar}_{time}.json",
                        min_version="0.13.2",
                    ),
                    VersionConstraintMapper(
                        "./{project}/{experiment}/map/{network}-{obsvar}_{layer}_{model}-{modvar}.json",
                        max_version="0.13.2",
                    ),
                ],
                ROUTE_SCATTER: [
                    VersionConstraintMapper(
                        "./{project}/{experiment}/scat/{network}-{obsvar}_{layer}_{model}-{modvar}_{time}.json",
                        min_version="0.13.2",
                    ),
                    VersionConstraintMapper(
                        "./{project}/{experiment}/scat/{network}-{obsvar}_{layer}_{model}-{modvar}.json",
                        max_version="0.13.2",
                    ),
                ],
                ROUTE_PROFILES: "./{project}/{experiment}/profiles/{location}_{network}_{obsvar}.json",
                ROUTE_HEATMAP_TIMESERIES: [
                    VersionConstraintMapper(
                        "./{project}/{experiment}/hm/ts/{region}-{network}-{obsvar}-{layer}.json",
                        min_version="0.13.2",  # https://github.com/metno/pyaerocom/blob/4478b4eafb96f0ca9fd722be378c9711ae10c1f6/setup.cfg
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
                ROUTE_FORECAST: "./{project}/{experiment}/forecast/{region}_{network}-{obsvar}_{layer}.json",
                ROUTE_GRIDDED_MAP: "./{project}/{experiment}/contour/{obsvar}_{model}.json",
                ROUTE_REPORT: "./reports/{project}/{experiment}/{title}.json",
                ROUTE_REPORT_IMAGE: "./reports/{project}/{experiment}/{path}",
            },
            version_provider=self._get_version,
        )

        self.FILTERS: dict[str, Callable[..., Awaitable[Any]]] = {
            ROUTE_REG_STATS: filter_regional_stats,
            ROUTE_HEATMAP: filter_heatmap,
            # ROUTE_CONTOUR: filter_contour,
            ROUTE_MAP: filter_map,
        }

    async def _load_json(
        self,
        file_path,
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

        json_str = self._cache.get(file_path, bypass_cache=not cache)
        # json_str = self._cache.get_json(file_path, no_cache=not cache)
        if access_type == AccessType.JSON_STR:
            return json_str

        elif access_type == AccessType.OBJ:
            return simplejson.loads(json_str, allow_nan=True)

        raise UnsupportedOperation(f"{access_type}")

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

    @async_and_sync
    async def _get_template(self, route: str, substitutions: dict) -> str:
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

    async def _get(
        self,
        route,
        route_args,
        **kwargs,
    ):
        use_caching = kwargs.pop("cache", False)
        default = kwargs.pop("default", None)
        _raise_file_not_found_error = kwargs.pop("_raise_file_not_found_error", True)
        access_type = self._normalize_access_type(kwargs.pop("access_type", None))

        substitutions = route_args | kwargs

        logger.debug(f"Fetching data for {route}.")

        path_template = await self._get_template(route, substitutions)
        logger.debug(f"Using template string {path_template}")

        relative_path = path_template.format(**substitutions)

        file_path = os.path.join(self._basedir, relative_path)
        logger.debug(f"Fetching file {file_path} as {access_type}-")

        filter_func = self.FILTERS.get(route, None)
        filter_vars = route_args | kwargs

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

            return await self._load_json(
                file_path, access_type=access_type, cache=use_caching
            )

        if access_type == AccessType.FILE_PATH:
            raise UnsupportedOperation("Filtered endpoints can not return a filepath")

        if access_type == AccessType.MTIME:
            return datetime.datetime.fromtimestamp(os.path.getmtime(file_path))
        if access_type == AccessType.CTIME:
            return datetime.datetime.fromtimestamp(os.path.getctime(file_path))

        obj = await self._load_json(
            file_path, access_type=AccessType.OBJ, cache=use_caching
        )

        obj = filter_func(obj, **filter_vars)

        if access_type == AccessType.OBJ:
            return obj

        if access_type == AccessType.JSON_STR:
            return json_dumps_wrapper(obj)

        raise UnsupportedOperation

    async def _put(self, obj, route, route_args, **kwargs):
        """Jsondb implemention of database put operation.

        If obj is string, it is assumed to be a wellformatted json string.
        Otherwise it is assumed to be a serializable python object.
        """
        substitutions = route_args | kwargs

        path_template = await self._get_template(route, substitutions)
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
            ROUTE_REG_STATS,
            {"project": project, "experiment": experiment, "frequency": frequency},
            access_type=kwargs.get("access_type", AccessType.OBJ),
            network=network,
            variable=variable,
            layer=layer,
            cache=True,
        )

    @async_and_sync
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
            ROUTE_HEATMAP,
            {"project": project, "experiment": experiment, "frequency": frequency},
            access_type=kwargs.get("access_type", AccessType.OBJ),
            region=region,
            time=time,
            cache=True,
        )

    @async_and_sync
    async def _get_uri_for_file(self, file_path: str) -> str:
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
            path = ":".join(split[3:])
            uri = build_uri(
                ROUTE_REPORT_IMAGE,
                {"project": project, "experiment": experiment, "path": path},
                {},
            )
            return uri

        for route in self.PATH_LOOKUP._lookuptable:
            if not (route == ROUTE_MODELS_STYLE):
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

            template = await self._get_template(route, subs)

            if "experiment" in subs:
                version = await self._get_version(subs["project"], subs["experiment"])
            else:
                # Project level models style does not have a version because version is defined
                # per experiment. version doesn't matter for models-style because it is priority
                # based, so we set a dummy value to simplify.
                version = Version("0.0.1")
            route_arg_names = extract_substitutions(route)

            try:
                all_args = parse_formatted_string(template, f"./{file_path}")  # type: ignore
                for k, v in all_args.items():
                    all_args[k] = v.replace("/", ":")

                route_args = {k: v for k, v in all_args.items() if k in route_arg_names}
                kwargs = {
                    k: v for k, v in all_args.items() if not (k in route_arg_names)
                }
            except Exception:
                continue
            else:
                uri = build_uri(route, route_args, kwargs | {"version": str(version)})
                return uri

        raise ValueError(f"Unable to build URI for file path {file_path}")

    @async_and_sync
    async def list_glob_stats(
        self,
        project: str,
        experiment: str,
        /,
        access_type: str | AccessType = AccessType.URI,
    ):
        access_type = self._normalize_access_type(access_type)
        if access_type in [AccessType.OBJ, AccessType.JSON_STR]:
            raise UnsupportedOperation(f"Unsupported accesstype, {access_type}")

        template = str(
            os.path.abspath(
                os.path.join(
                    self._basedir,
                    await self._get_template(
                        ROUTE_GLOB_STATS, {"project": project, "experiment": experiment}
                    ),  # type: ignore
                )
            )
        )
        glb = template.replace("{frequency}", "*")

        glb = glb.format(project=project, experiment=experiment)

        result = []
        for f in glob.glob(glb):
            if access_type == AccessType.FILE_PATH:
                result.append(f)
                continue

            result.append(await self._get_uri_for_file(f))

        return result

    @async_and_sync
    async def list_timeseries(
        self,
        project: str,
        experiment: str,
        /,
        access_type: str | AccessType = AccessType.URI,
    ):
        access_type = self._normalize_access_type(access_type)
        if access_type in [AccessType.OBJ, AccessType.JSON_STR]:
            raise UnsupportedOperation(f"Unsupported accesstype, {access_type}")

        template = str(
            os.path.abspath(
                os.path.join(
                    self._basedir,
                    await self._get_template(
                        ROUTE_TIMESERIES,
                        {"project": project, "experiment": experiment},
                    ),  # type: ignore
                )
            )
        )
        glb = (
            template.replace("{location}", "*")
            .replace("{network}", "*")
            .replace("{obsvar}", "*")
            .replace("{layer}", "*")
        )
        glb = glb.format(project=project, experiment=experiment)

        result = []
        for f in glob.glob(glb):
            if access_type == AccessType.FILE_PATH:
                result.append(f)
                continue

            result.append(await self._get_uri_for_file(f))

        return result

    @async_and_sync
    async def list_map(
        self,
        project: str,
        experiment: str,
        /,
        access_type: str | AccessType = AccessType.URI,
    ):
        access_type = self._normalize_access_type(access_type)
        if access_type in [AccessType.OBJ, AccessType.JSON_STR]:
            raise UnsupportedOperation(f"Unsupported accesstype, {access_type}")

        template = str(
            os.path.abspath(
                os.path.join(
                    self._basedir,
                    self._get_template(
                        ROUTE_MAP,
                        {"project": project, "experiment": experiment},
                    ),  # type: ignore
                )
            )
        )
        glb = (
            template.replace("{network}", "*")
            .replace("{obsvar}", "*")
            .replace("{layer}", "*")
            .replace("{model}", "*")
            .replace("{modvar}", "*")
            .replace("{time}", "*")
        )
        glb = glb.format(project=project, experiment=experiment)

        result = []
        for f in glob.glob(glb):
            if access_type == AccessType.FILE_PATH:
                result.append(f)
                continue

            result.append(await self._get_uri_for_file(f))

        return result

    @async_and_sync
    async def get_by_uri(
        self,
        uri: str,
        /,
        access_type: str | AccessType = AccessType.OBJ,
        cache: bool = False,
        default=None,
    ):
        access_type = self._normalize_access_type(access_type)
        if access_type in [AccessType.URI]:
            return uri

        route, route_args, kwargs = parse_uri(uri)

        if route.startswith("/v0/report-image/"):
            return await self.get_report_image(
                route_args["project"],
                route_args["experiment"],
                route_args["path"],
                access_type=access_type,
            )

        if route.startswith("/v0/map-overlay/"):
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
    async def put_by_uri(self, obj, uri: str):
        route, route_args, kwargs = parse_uri(uri)

        if route.startswith("/v0/report-image/"):
            await self.put_report_image(
                obj, route_args["project"], route_args["experiment"], route_args["path"]
            )
            return

        if route.startswith("/v0/map-overlay/"):
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
        os.makedirs(os.path.expanduser("~/.aerovaldb/.lock/"), exist_ok=True)
        lock_file = os.path.join(
            os.environ.get("AVDB_LOCK_DIR", os.path.expanduser("~/.aerovaldb/.lock/")),
            md5(self._basedir.encode()).hexdigest(),
        )
        return lock_file

    def lock(self):
        if self._use_real_lock:
            return FileLock(self._get_lock_file())

        return FakeLock()

    @async_and_sync
    async def list_all(self, access_type: str | AccessType = AccessType.URI):
        access_type = self._normalize_access_type(access_type)

        if access_type in [AccessType.OBJ, AccessType.JSON_STR]:
            UnsupportedOperation(f"Accesstype {access_type} not supported.")

        glb = glob.iglob(os.path.join(self._basedir, "./**"), recursive=True)

        result = []
        for f in glb:
            if os.path.isfile(f):
                if access_type == AccessType.FILE_PATH:
                    result.append(f)
                    continue

                try:
                    uri = await self._get_uri_for_file(f)
                except (ValueError, KeyError):
                    continue
                else:
                    result.append(uri)

        return result

    @async_and_sync
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
                route=ROUTE_REPORT_IMAGE,
                route_args={
                    "project": project,
                    "experiment": experiment,
                    "path": path,
                },
                access_type=access_type,
            )
        file_path = await self._get(
            route=ROUTE_REPORT_IMAGE,
            route_args={
                "project": project,
                "experiment": experiment,
                "path": path,
            },
            access_type=AccessType.FILE_PATH,
        )
        logger.debug(f"Fetching image with path '{file_path}'")

        if access_type == AccessType.FILE_PATH:
            return file_path

        with open(file_path, "rb") as f:
            return f.read()

    @async_and_sync
    async def put_report_image(self, obj, project: str, experiment: str, path: str):
        template = await self._get_template(ROUTE_REPORT_IMAGE, {})

        file_path = os.path.join(
            self._basedir,
            template.format(project=project, experiment=experiment, path=path),
        )
        os.makedirs(Path(file_path).parent, exist_ok=True)
        with open(file_path, "wb") as f:
            f.write(obj)

    @async_and_sync
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
                route=ROUTE_MAP_OVERLAY,
                route_args={
                    "project": project,
                    "experiment": experiment,
                    "source": source,
                    "variable": variable,
                    "date": date,
                },
                _raise_file_not_found_error=False,
                access_type=AccessType.FILE_PATH,
            )

            file_path += ext
            if os.path.exists(file_path):
                break

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
        template = await self._get_template(ROUTE_MAP_OVERLAY, {})

        file_path = os.path.join(
            self._basedir,
            template.format(
                project=project,
                experiment=experiment,
                source=source,
                variable=variable,
                date=date,
            ),
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
    async def get_contour(
        self,
        project: str,
        experiment: str,
        obsvar: str,
        model: str,
        /,
        *args,
        timestep: str | None = None,
        access_type: str | AccessType = AccessType.OBJ,
        cache: bool = False,
        default=None,
        **kwargs,
    ):
        access_type = self._normalize_access_type(access_type)

        try:
            file_path = await self._get(
                ROUTE_CONTOUR,
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

                result = simplejson.loads(self._cache.get(key))
            except CacheMissError:
                result = await self._get(
                    ROUTE_CONTOUR,
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
                ROUTE_CONTOUR2,
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
                ROUTE_CONTOUR,
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
            ROUTE_CONTOUR2,
            {
                "project": project,
                "experiment": experiment,
                "obsvar": obsvar,
                "model": model,
                "timestep": timestep,
            },
        )
