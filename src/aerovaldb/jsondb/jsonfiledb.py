import glob
import logging
import os
from enum import Enum
from pathlib import Path
from typing import Callable, Awaitable, Any

import orjson
from async_lru import alru_cache
from packaging.version import Version

from aerovaldb.aerovaldb import AerovalDB, get_method, put_method
from aerovaldb.exceptions import FileDoesNotExist, UnusedArguments, TemplateNotFound
from aerovaldb.types import AccessType

from ..utils import async_and_sync
from .templatemapper import (
    TemplateMapper,
    DataVersionToTemplateMapper,
    PriorityDataVersionToTemplateMapper,
    SkipMapper,
)
from .filter import filter_heatmap, filter_regional_stats
from ..exceptions import UnsupportedOperation
from .cache import JSONLRUCache

logger = logging.getLogger(__name__)


class AerovalJsonFileDB(AerovalDB):
    def __init__(self, basedir: str | Path):
        self._cache = JSONLRUCache(max_size=64)

        self._basedir = basedir
        if isinstance(self._basedir, str):
            self._basedir = Path(self._basedir)

        self.PATH_LOOKUP: dict[str, list[TemplateMapper]] = {
            "/v0/glob_stats/{project}/{experiment}/{frequency}": [
                DataVersionToTemplateMapper(
                    "./{project}/{experiment}/hm/glob_stats_{frequency}.json",
                    version_provider=self._get_version,
                )
            ],
            "/v0/regional_stats/{project}/{experiment}/{frequency}": [
                # Same as glob_stats
                DataVersionToTemplateMapper(
                    "./{project}/{experiment}/hm/glob_stats_{frequency}.json",
                    version_provider=self._get_version,
                )
            ],
            "/v0/heatmap/{project}/{experiment}/{frequency}": [
                # Same as glob_stats
                DataVersionToTemplateMapper(
                    "./{project}/{experiment}/hm/glob_stats_{frequency}.json",
                    version_provider=self._get_version,
                )
            ],
            "/v0/contour/{project}/{experiment}/{obsvar}/{model}": [
                DataVersionToTemplateMapper(
                    "./{project}/{experiment}/contour/{obsvar}_{model}.geojson",
                    version_provider=self._get_version,
                )
            ],
            "/v0/ts/{project}/{experiment}/{location}/{network}/{obsvar}/{layer}": [
                DataVersionToTemplateMapper(
                    "./{project}/{experiment}/ts/{location}_{network}-{obsvar}_{layer}.json",
                    version_provider=self._get_version,
                )
            ],
            "/v0/experiments/{project}": [
                PriorityDataVersionToTemplateMapper(["./{project}/experiments.json"])
            ],
            "/v0/config/{project}/{experiment}": [
                PriorityDataVersionToTemplateMapper(
                    ["./{project}/{experiment}/cfg_{project}_{experiment}.json"]
                )
            ],
            "/v0/menu/{project}/{experiment}": [
                DataVersionToTemplateMapper(
                    "./{project}/{experiment}/menu.json",
                    version_provider=self._get_version,
                )
            ],
            "/v0/statistics/{project}/{experiment}": [
                DataVersionToTemplateMapper(
                    "./{project}/{experiment}/statistics.json",
                    version_provider=self._get_version,
                )
            ],
            "/v0/ranges/{project}/{experiment}": [
                DataVersionToTemplateMapper(
                    "./{project}/{experiment}/ranges.json",
                    version_provider=self._get_version,
                )
            ],
            "/v0/regions/{project}/{experiment}": [
                DataVersionToTemplateMapper(
                    "./{project}/{experiment}/regions.json",
                    version_provider=self._get_version,
                )
            ],
            "/v0/ts_weekly/{project}/{experiment}/{location}_{network}-{obsvar}_{layer}": [
                DataVersionToTemplateMapper(
                    "./{project}/{experiment}/ts/diurnal/{location}_{network}-{obsvar}_{layer}.json",
                    version_provider=self._get_version,
                )
            ],
            "/v0/profiles/{project}/{experiment}/{location}/{network}/{obsvar}": [
                DataVersionToTemplateMapper(
                    "./{project}/{experiment}/profiles/{location}_{network}-{obsvar}.json",
                    version_provider=self._get_version,
                )
            ],
            "/v0/forecast/{project}/{experiment}/{region}/{network}/{obsvar}/{layer}": [
                DataVersionToTemplateMapper(
                    "./{project}/{experiment}/forecast/{region}_{network}-{obsvar}_{layer}.json",
                    version_provider=self._get_version,
                )
            ],
            "/v0/gridded_map/{project}/{experiment}/{obsvar}/{model}": [
                DataVersionToTemplateMapper(
                    "./{project}/{experiment}/contour/{obsvar}_{model}.json",
                    version_provider=self._get_version,
                )
            ],
            "/v0/report/{project}/{experiment}/{title}": [
                DataVersionToTemplateMapper(
                    "./reports/{project}/{experiment}/{title}.json",
                    version_provider=self._get_version,
                )
            ],
            "/v0/map/{project}/{experiment}/{network}/{obsvar}/{layer}/{model}/{modvar}": [
                DataVersionToTemplateMapper(
                    "./{project}/{experiment}/map/{network}-{obsvar}_{layer}_{model}-{modvar}_{time}.json",
                    min_version="0.13.2",
                    version_provider=self._get_version,
                ),
                DataVersionToTemplateMapper(
                    "./{project}/{experiment}/map/{network}-{obsvar}_{layer}_{model}-{modvar}.json",
                    max_version="0.13.2",
                    version_provider=self._get_version,
                ),
            ],
            "/v0/scat/{project}/{experiment}/{network}/{obsvar}/{layer}/{model}/{modvar}/{time}": [
                DataVersionToTemplateMapper(
                    "./{project}/{experiment}/scat/{network}-{obsvar}_{layer}_{model}-{modvar}_{time}.json",
                    min_version="0.13.2",
                    version_provider=self._get_version,
                ),
                DataVersionToTemplateMapper(
                    "./{project}/{experiment}/scat/{network}-{obsvar}_{layer}_{model}-{modvar}.json",
                    max_version="0.13.2",
                    version_provider=self._get_version,
                ),
            ],
            "/v0/hm_ts/{project}/{experiment}/{region}/{network}/{obsvar}/{layer}": [
                DataVersionToTemplateMapper(
                    "./{project}/{experiment}/hm/ts/{region}-{network}-{obsvar}-{layer}.json",
                    min_version="0.13.2",  # https://github.com/metno/pyaerocom/blob/4478b4eafb96f0ca9fd722be378c9711ae10c1f6/setup.cfg
                    version_provider=self._get_version,
                ),
                DataVersionToTemplateMapper(
                    "./{project}/{experiment}/hm/ts/{network}-{obsvar}-{layer}.json",
                    min_version="0.12.2",
                    max_version="0.13.2",
                    version_provider=self._get_version,
                ),
                DataVersionToTemplateMapper(
                    "./{project}/{experiment}/hm/ts/stats_ts.json",
                    max_version="0.12.2",
                    version_provider=self._get_version,
                ),
            ],
            "/v0/model_style/{project}": [
                PriorityDataVersionToTemplateMapper(
                    [
                        "./{project}/{experiment}/models-style.json",
                        "./{project}/models-style.json",
                    ]
                )
            ],
        }

        self.FILTERS: dict[str, Callable[..., Awaitable[Any]]] = {
            "/v0/regional_stats/{project}/{experiment}/{frequency}": filter_regional_stats,
            "/v0/heatmap/{project}/{experiment}/{frequency}": filter_heatmap,
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
            return Version("0.0.1")
        except orjson.JSONDecodeError:
            # Work around for https://github.com/metno/aerovaldb/issues/28
            return Version("0.14.0")

        try:
            version_str = config["exp_info"]["pyaerocom_version"]
            version = Version(version_str)
        except KeyError:
            version = Version("0.0.1")

        return version

    def _normalize_access_type(
        self, access_type: AccessType | str | None, default: AccessType = AccessType.OBJ
    ) -> AccessType:
        """Normalizes the access_type to an instance of AccessType enum.

        :param access_type: AccessType instance or string convertible to AccessType
        :param default: The type to return if access_type is None. Defaults to AccessType.OBJ
        :raises ValueError: If str access_type can't be converted to AccessType.
        :raises ValueError: If access_type is not str or AccessType
        :return: The normalized AccessType.
        """
        if isinstance(access_type, AccessType):
            return access_type

        if isinstance(access_type, str):
            try:
                return AccessType[access_type]
            except:
                raise ValueError(
                    f"String '{access_type}' can not be converted to AccessType."
                )
        if access_type is None:
            return default

        raise ValueError(
            f"Access_type, {access_type}, could not be normalized. This is probably due to input that is not a str or AccessType instance."
        )

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
        file_path_template = None
        for f in self.PATH_LOOKUP[route]:
            try:
                file_path_template = await f(**substitutions)
            except SkipMapper:
                continue

            break

        if file_path_template is None:
            raise TemplateNotFound("No template found.")

        return file_path_template

    async def _get(
        self,
        route,
        route_args,
        *args,
        **kwargs,
    ):
        use_caching = kwargs.get("cache", False)
        if len(args) > 0:
            raise UnusedArguments(
                f"Unexpected positional arguments {args}. Jsondb does not use additional positional arguments currently."
            )
        logger.debug(f"Fetching data for {route}.")
        substitutions = route_args | kwargs
        path_template = await self._get_template(route, substitutions)
        logger.debug(f"Using template string {path_template}")

        relative_path = path_template.format(**substitutions)

        access_type = self._normalize_access_type(kwargs.pop("access_type", None))

        file_path = Path(os.path.join(self._basedir, relative_path)).resolve()
        logger.debug(f"Fetching file {file_path} as {access_type}-")

        filter_func = self.FILTERS.get(route, None)
        filter_vars = route_args | kwargs

        if access_type == AccessType.FILE_PATH:
            if filter_func is not None:
                raise UnsupportedOperation(
                    "Filtered endpoints can not return a file path."
                )

            if not os.path.exists(file_path):
                raise FileDoesNotExist(f"File {file_path} does not exist.")
            return file_path

        if access_type == AccessType.JSON_STR:
            if filter_func is None:
                return await self._cache.get_json(file_path, no_cache=not use_caching)

            raw = await self._cache.get_json(file_path, no_cache=not use_caching)
            obj = orjson.loads(raw)
            filtered = filter_func(obj, **filter_vars)
            return orjson.dumps(filtered)

        raw = await self._cache.get_json(file_path, no_cache=not use_caching)

        if filter_func is None:
            return orjson.loads(raw)

        return filter_func(
            orjson.loads(
                await self._cache.get_json(file_path, no_cache=not use_caching)
            ),
            **filter_vars,
        )

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
        path_template = await self._get_template(route, substitutions)
        relative_path = path_template.format(**substitutions)

        file_path = Path(os.path.join(self._basedir, relative_path)).resolve()

        logger.debug(f"Mapped route {route} / { route_args} to file {file_path}.")

        if not os.path.exists(os.path.dirname(file_path)):
            os.makedirs(os.path.dirname(file_path))

        if isinstance(obj, str):
            json = obj
        else:
            json = orjson.dumps(obj)
        with open(file_path, "wb") as f:
            f.write(json)

    @async_and_sync
    async def get_experiments(self, project: str, /, *args, **kwargs):
        experiments = {}
        for exp in self._list_experiments(project, has_results=True):
            config = await self.get_config(project, exp)
            public = config["exp_info"]["public"]
            experiments[exp] = {"public": public}

        access_type = self._normalize_access_type(kwargs.pop("access_type", None))

        if access_type == AccessType.FILE_PATH:
            raise UnsupportedOperation(
                f"get_experiment() does not support access_type {access_type}."
            )

        if access_type == AccessType.JSON_STR:
            json = orjson.dumps(experiments)
            return json

        return experiments

    # @async_and_sync
    # async def get_menu(self, project: str, experiment: str, /, *args, **kwargs):
    #    raise NotImplementedError

    # @async_and_sync
    # async def get_ranges(self, project: str, experiment: str, /, *args, **kwargs):
    #    raise NotImplementedError

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
            "/v0/regional_stats/{project}/{experiment}/{frequency}",
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
            "/v0/heatmap/{project}/{experiment}/{frequency}",
            {"project": project, "experiment": experiment, "frequency": frequency},
            access_type=kwargs.get("access_type", AccessType.OBJ),
            region=region,
            time=time,
            cache=True,
        )

    def _list_experiments(
        self, project: str, /, has_results: bool = False
    ) -> list[str]:
        project_path = os.path.join(self._basedir, project)
        experiments = []

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
