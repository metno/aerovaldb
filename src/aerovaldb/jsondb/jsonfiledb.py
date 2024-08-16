import glob
import logging
import os
import shutil
from pathlib import Path
from typing import Callable, Awaitable, Any, Generator

from async_lru import alru_cache
from packaging.version import Version
from pkg_resources import DistributionNotFound, get_distribution  # type: ignore

from aerovaldb.aerovaldb import AerovalDB
from aerovaldb.exceptions import UnusedArguments
from aerovaldb.types import AccessType
from ..utils.string_mapper import StringMapper, VersionConstraintMapper

from ..utils import (
    async_and_sync,
    str_to_bool,
    validate_filename_component,
    json_dumps_wrapper,
    parse_uri,
    parse_formatted_string,
    build_uri,
    extract_substitutions,
    run_until_finished,
)
from ..utils.filter import filter_heatmap, filter_regional_stats
from ..exceptions import UnsupportedOperation
from .cache import JSONLRUCache
from ..routes import *
from ..lock import FakeLock, FileLock
from hashlib import md5
import simplejson  # type: ignore

logger = logging.getLogger(__name__)


class AerovalJsonFileDB(AerovalDB):
    def __init__(self, basedir: str | Path, /, use_async: bool = False):
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

        self._asyncio = use_async
        self._cache = JSONLRUCache(max_size=64, asyncio=self._asyncio)

        self._basedir = os.path.realpath(basedir)

        if not os.path.exists(self._basedir):
            os.makedirs(self._basedir)

        self.PATH_LOOKUP = StringMapper(
            {
                ROUTE_GLOB_STATS: "./{project}/{experiment}/hm/glob_stats_{frequency}.json",
                ROUTE_REG_STATS: "./{project}/{experiment}/hm/glob_stats_{frequency}.json",
                ROUTE_HEATMAP: "./{project}/{experiment}/hm/glob_stats_{frequency}.json",
                ROUTE_CONTOUR: "./{project}/{experiment}/contour/{obsvar}_{model}.geojson",
                ROUTE_TIMESERIES: "./{project}/{experiment}/ts/{location}_{network}-{obsvar}_{layer}.json",
                ROUTE_TIMESERIES_WEEKLY: "./{project}/{experiment}/ts/diurnal/{location}_{network}-{obsvar}_{layer}.json",
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
                ROUTE_PROFILES: "./{project}/{experiment}/profiles/{location}_{network}-{obsvar}.json",
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
        *args,
        **kwargs,
    ):
        use_caching = kwargs.pop("cache", False)
        default = kwargs.pop("default", None)
        access_type = self._normalize_access_type(kwargs.pop("access_type", None))

        if len(args) > 0:
            raise UnusedArguments(
                f"Unexpected positional arguments {args}. Jsondb does not use additional positional arguments currently."
            )

        substitutions = route_args | kwargs
        [validate_filename_component(x) for x in substitutions.values()]

        logger.debug(f"Fetching data for {route}.")

        path_template = await self._get_template(route, substitutions)
        logger.debug(f"Using template string {path_template}")

        relative_path = path_template.format(**substitutions)

        file_path = str(Path(os.path.join(self._basedir, relative_path)).resolve())
        logger.debug(f"Fetching file {file_path} as {access_type}-")

        filter_func = self.FILTERS.get(route, None)
        filter_vars = route_args | kwargs

        if not os.path.exists(file_path):
            if default is None or access_type == AccessType.FILE_PATH:
                raise FileNotFoundError(f"File {file_path} does not exist.")
            return default

        # No filtered.
        if filter_func is None:
            if access_type == AccessType.FILE_PATH:
                return file_path

            if access_type == AccessType.JSON_STR:
                raw = await self._cache.get_json(file_path, no_cache=not use_caching)
                return raw

            raw = await self._cache.get_json(file_path, no_cache=not use_caching)

            return simplejson.loads(raw, allow_nan=True)

        if access_type == AccessType.FILE_PATH:
            raise UnsupportedOperation("Filtered endpoints can not return a filepath")

        json = await self._cache.get_json(file_path, no_cache=not use_caching)
        obj = simplejson.loads(json, allow_nan=True)

        obj = filter_func(obj, **filter_vars)

        if access_type == AccessType.OBJ:
            return obj

        if access_type == AccessType.JSON_STR:
            return json_dumps_wrapper(obj)

        raise UnsupportedOperation

    async def _put(self, obj, route, route_args, *args, **kwargs):
        """Jsondb implemention of database put operation.

        If obj is string, it is assumed to be a wellformatted json string.
        Otherwise it is assumed to be a serializable python object.
        """
        if len(args) > 0:
            raise UnusedArguments(
                f"Unexpected positional arguments {args}. Jsondb does not use additional positional arguments currently."
            )

        substitutions = route_args | kwargs
        [validate_filename_component(x) for x in substitutions.values()]

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

    @async_and_sync
    async def get_experiments(self, project: str, /, *args, exp_order=None, **kwargs):
        # If an experiments.json file exists, read it.
        try:
            access_type = self._normalize_access_type(kwargs.pop("access_type", None))
            experiments = await self._get(
                ROUTE_EXPERIMENTS,
                {"project": project},
                access_type=access_type,
            )
        except FileNotFoundError:
            pass
        else:
            return experiments

        # Otherwise generate it based on config and expinfo.public information.
        experiments = {}
        for exp in self._list_experiments(project, has_results=True):
            public = False
            try:
                config = await self.get_config(project, exp)
            except FileNotFoundError:
                pass
            else:
                public = config.get("exp_info", {}).get("public", False)
            experiments[exp] = {"public": public}

        access_type = self._normalize_access_type(kwargs.pop("access_type", None))

        experiments = dict(experiments.items())
        if access_type == AccessType.FILE_PATH:
            raise UnsupportedOperation(
                f"get_experiments() does not support access_type {access_type}."
            )

        if access_type == AccessType.JSON_STR:
            json = json_dumps_wrapper(experiments)
            return json

        return experiments

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

    def _get_uri_for_file(self, file_path: str) -> str:
        """
        For the provided data file path, returns the corresponding
        URI.

        :param file_path : The file_path.
        """
        file_path = os.path.join(self._basedir, file_path)
        file_path = os.path.relpath(file_path, start=self._basedir)

        for route in self.PATH_LOOKUP._lookuptable:
            if not (route == ROUTE_MODELS_STYLE):
                if file_path.startswith("reports/"):
                    str = "/".join(file_path.split("/")[1:3])
                    subs = parse_formatted_string("{project}/{experiment}", str)
                else:
                    str = "/".join(file_path.split("/")[0:2])
                    subs = parse_formatted_string("{project}/{experiment}", str)

                template = self._get_template(route, subs)
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

                template = self._get_template(route, subs)

            # TODO: Ugly hack need to fix
            try:
                version = run_until_finished(
                    self._get_version(subs["project"], subs["experiment"])
                )
            except TypeError:
                version = self._get_version(subs["project"], subs["experiment"])
            except KeyError:
                version = Version("0.0.1")
            route_arg_names = extract_substitutions(route)

            try:
                all_args = parse_formatted_string(template, f"./{file_path}")  # type: ignore

                route_args = {k: v for k, v in all_args.items() if k in route_arg_names}
                kwargs = {
                    k: v for k, v in all_args.items() if not (k in route_arg_names)
                }
            except Exception:
                continue
            else:
                return build_uri(route, route_args, kwargs | {"version": version})

        raise ValueError(f"Unable to build URI for file path {file_path}")

    def list_glob_stats(
        self,
        project: str,
        experiment: str,
        /,
        access_type: str | AccessType = AccessType.URI,
    ) -> Generator[str, None, None]:
        access_type = self._normalize_access_type(access_type)
        if access_type in [AccessType.OBJ, AccessType.JSON_STR]:
            raise UnsupportedOperation(f"Unsupported accesstype, {access_type}")

        template = str(
            os.path.realpath(
                os.path.join(
                    self._basedir,
                    self._get_template(
                        ROUTE_GLOB_STATS, {"project": project, "experiment": experiment}
                    ),  # type: ignore
                )
            )
        )
        glb = template.replace("{frequency}", "*")

        glb = glb.format(project=project, experiment=experiment)
        for f in glob.glob(glb):
            if access_type == AccessType.FILE_PATH:
                yield f
                continue

            yield self._get_uri_for_file(f)

    def list_timeseries(
        self,
        project: str,
        experiment: str,
        /,
        access_type: str | AccessType = AccessType.URI,
    ) -> Generator[str, None, None]:
        access_type = self._normalize_access_type(access_type)
        if access_type in [AccessType.OBJ, AccessType.JSON_STR]:
            raise UnsupportedOperation(f"Unsupported accesstype, {access_type}")

        template = str(
            os.path.realpath(
                os.path.join(
                    self._basedir,
                    self._get_template(
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

        for f in glob.glob(glb):
            if access_type == AccessType.FILE_PATH:
                yield f
                continue

            yield self._get_uri_for_file(f)

    def list_map(
        self,
        project: str,
        experiment: str,
        /,
        access_type: str | AccessType = AccessType.URI,
    ) -> Generator[str, None, None]:
        access_type = self._normalize_access_type(access_type)
        if access_type in [AccessType.OBJ, AccessType.JSON_STR]:
            raise UnsupportedOperation(f"Unsupported accesstype, {access_type}")

        template = str(
            os.path.realpath(
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

        for f in glob.glob(glb):
            if access_type == AccessType.FILE_PATH:
                yield f
                continue

            yield self._get_uri_for_file(f)

    def _list_experiments(
        self, project: str, /, has_results: bool = False
    ) -> list[str]:
        project_path = os.path.join(self._basedir, project)
        experiments = []

        if not os.path.exists(project_path):
            return []

        for f in os.listdir(project_path):
            if not has_results:
                if os.path.isdir(os.path.join(project_path, f)):
                    experiments.append(f)
            else:
                if not os.path.isdir(os.path.join(project_path, f)):
                    continue
                glb = os.path.join(project_path, f, "map", "*.json")
                if len(glob.glob(glb)) == 0:
                    continue

                experiments.append(f)

        return experiments

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

    def list_all(self, access_type: str | AccessType = AccessType.URI):
        access_type = self._normalize_access_type(access_type)

        if access_type in [AccessType.OBJ, AccessType.JSON_STR]:
            UnsupportedOperation(f"Accesstype {access_type} not supported.")

        glb = glob.iglob(os.path.join(self._basedir, "./**"), recursive=True)

        for f in glb:
            if os.path.isfile(f):
                if access_type == AccessType.FILE_PATH:
                    yield f
                    continue

                try:
                    uri = self._get_uri_for_file(f)
                except (ValueError, KeyError):
                    continue
                else:
                    yield uri
