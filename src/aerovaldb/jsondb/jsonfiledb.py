import abc
import json
import logging
import os
import string
from enum import Enum
from pathlib import Path
from typing import Callable

import aiofile
import orjson
from async_lru import alru_cache
from packaging.version import Version

from aerovaldb.aerovaldb import AerovalDB, get_method, put_method
from aerovaldb.exceptions import FileDoesNotExist, UnusedArguments
from aerovaldb.types import AccessType

from ..utils import async_and_sync

logger = logging.getLogger(__name__)


VersionProvider = Callable[[str, str], Version]


class SkipMapper(Exception):
    pass


class TemplateMapper(abc.ABC):
    """
    This class is a base class for objects that implement a
    file path template selection algorithm. Inheriting
    implementations should implement the __call_ function,
    and raising SkipMapper if the implementation can't or
    won't handle the request.
    """

    @async_and_sync
    async def __call__(self, *args, version_provider: VersionProvider, **kwargs) -> str:
        raise NotImplementedError


class DataVersionToTemplateMapper(TemplateMapper):
    """
    This class returns its provided template if the
    data version read from a config file matches
    the configured bounds of this class.
    """

    def __init__(
        self,
        template: str,
        *,
        min_version: str | None = None,
        max_version: str | None = None,
    ):
        self.min_version = None
        self.max_version = None

        if min_version is not None:
            self.min_version = Version(min_version)
        if max_version is not None:
            self.max_version = Version(max_version)

        self.template = template

    @async_and_sync
    async def __call__(self, *args, version_provider: VersionProvider, **kwargs) -> str:
        version = await version_provider(kwargs["project"], kwargs["experiment"])
        if self.min_version is not None and version < self.min_version:
            raise SkipMapper
        if self.max_version is not None and version > self.max_version:
            raise SkipMapper

        return self.template


class PriorityDataVersionToTemplateMapper(TemplateMapper):
    """
    This class takes a list of templates, trying them in turn
    and returning the first template that fits the provided
    parameters.
    """

    def __init__(self, templates: list[str]):
        self.templates = templates

    @async_and_sync
    async def __call__(self, *args, version_provider: VersionProvider, **kwargs) -> str:
        selected_template = None
        for t in self.templates:
            try:
                t.format(**kwargs)
                selected_template = t
                break
            except:
                continue

        if selected_template is None:
            raise SkipMapper

        return selected_template


class AerovalJsonFileDB(AerovalDB):
    def __init__(self, basedir: str | Path):
        self._basedir = basedir
        if isinstance(self._basedir, str):
            self._basedir = Path(self._basedir)

        self.PATH_LOOKUP: dict[str, TemplateMapper] = {
            "/v0/glob_stats/{project}/{experiment}/{frequency}": [
                DataVersionToTemplateMapper(
                    "./{project}/{experiment}/hm/glob_stats_{frequency}.json"
                )
            ],
            "/v0/contour/{project}/{experiment}/{obsvar}/{model}": [
                DataVersionToTemplateMapper(
                    "./{project}/{experiment}/contour/{obsvar}_{model}.geojson"
                )
            ],
            "/v0/ts/{project}/{experiment}/{location}/{network}/{obsvar}/{layer}": [
                DataVersionToTemplateMapper(
                    "./{project}/{experiment}/ts/{location}_{network}-{obsvar}_{layer}.json"
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
                DataVersionToTemplateMapper("./{project}/{experiment}/menu.json")
            ],
            "/v0/statistics/{project}/{experiment}": [
                DataVersionToTemplateMapper("./{project}/{experiment}/statistics.json")
            ],
            "/v0/ranges/{project}/{experiment}": [
                DataVersionToTemplateMapper("./{project}/{experiment}/ranges.json")
            ],
            "/v0/regions/{project}/{experiment}": [
                DataVersionToTemplateMapper("./{project}/{experiment}/regions.json")
            ],
            "/v0/ts_weekly/{project}/{experiment}/{location}_{network}-{obsvar}_{layer}": [
                DataVersionToTemplateMapper(
                    "./{project}/{experiment}/ts/diurnal/{location}_{network}-{obsvar}_{layer}.json"
                )
            ],
            "/v0/profiles/{project}/{experiment}/{location}/{network}/{obsvar}": [
                DataVersionToTemplateMapper(
                    "./{project}/{experiment}/profiles/{location}_{network}-{obsvar}.json"
                )
            ],
            "/v0/forecast/{project}/{experiment}/{region}/{network}/{obsvar}/{layer}": [
                DataVersionToTemplateMapper(
                    "./{project}/{experiment}/forecast/{region}_{network}-{obsvar}_{layer}.json"
                )
            ],
            "/v0/gridded_map/{project}/{experiment}/{obsvar}/{model}": [
                DataVersionToTemplateMapper(
                    "./{project}/{experiment}/contour/{obsvar}_{model}.json"
                )
            ],
            "/v0/report/{project}/{experiment}/{title}": [
                DataVersionToTemplateMapper(
                    "./reports/{project}/{experiment}/{title}.json"
                )
            ],
            "/v0/map/{project}/{experiment}/{network}/{obsvar}/{layer}/{model}/{modvar}": [
                DataVersionToTemplateMapper(
                    "./{project}/{experiment}/map/{network}-{obsvar}_{layer}_{model}-{modvar}_{time}.json",
                    min_version="0.13.2",
                ),
                DataVersionToTemplateMapper(
                    "./{project}/{experiment}/map/{network}-{obsvar}_{layer}_{model}-{modvar}.json",
                    max_version="0.13.1",
                ),
            ],
            "/v0/scat/{project}/{experiment}/{network}-{obsvar}_{layer}_{model}-{modvar}": [
                DataVersionToTemplateMapper(
                    "./{project}/{experiment}/scat/{network}-{obsvar}_{layer}_{model}-{modvar}_{time}.json",
                    min_version="0.13.2",
                ),
                DataVersionToTemplateMapper(
                    "./{project}/{experiment}/scat/{network}-{obsvar}_{layer}_{model}-{modvar}.json",
                    max_version="0.13.1",
                ),
            ],
            "/v0/hm_ts/{project}/{experiment}": [
                PriorityDataVersionToTemplateMapper(
                    [
                        "./{project}/{experiment}/hm/ts/{location}-{network}-{obsvar}-{layer}.json",
                        "./{project}/{experiment}/hm/ts/{network}-{obsvar}-{layer}.json",
                        "./{project}/{experiment}/hm/ts/stats_ts.json",
                    ]
                )
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
        data = await self.get_config(project, experiment)

        try:
            version_str = data["exp_info"]["pyaerocom_version"]
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
        logger.info(f"Test 1 - {access_type}")
        if isinstance(access_type, AccessType):
            logger.info("Test 2")

            return access_type
        if isinstance(access_type, str):
            logger.info("Test 3")

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

    async def _get(self, route, route_args, *args, **kwargs):
        if len(args) > 0:
            raise UnusedArguments(
                f"Unexpected positional arguments {args}. Jsondb does not use additional positional arguments currently."
            )

        substitutions = route_args | kwargs

        file_template = None
        for f in self.PATH_LOOKUP[route]:
            try:
                file_template = await f(
                    **substitutions, version_provider=self._get_version
                )
            except SkipMapper:
                continue

            break

        if file_template is None:
            raise Exception("No template found.")

        relative_path = file_template.format(**substitutions)

        access_type = self._normalize_access_type(kwargs.pop("access_type", None))

        file_path = Path(os.path.join(self._basedir, relative_path)).resolve()

        logger.debug(
            f"Mapped route {route} / { route_args} to file {file_path} with type {access_type}."
        )

        if access_type == AccessType.FILE_PATH:
            if not os.path.exists(file_path):
                raise FileDoesNotExist(f"File {file_path} does not exist.")
            return file_path

        if access_type == AccessType.JSON_STR:
            async with aiofile.async_open(file_path, "r") as f:
                raw = await f.read()

            return raw

        async with aiofile.async_open(file_path, "r") as f:
            raw = await f.read()

        return orjson.loads(raw)

    def _put(self, obj, route, route_args, *args, **kwargs):
        """Jsondb implemention of database put operation.

        If obj is string, it is assumed to be a wellformatted json string.
        Otherwise it is assumed to be a serializable python object.
        """
        file_path = self._get_file_path_from_route(route, route_args, **kwargs)
        logger.debug(f"Mapped route {route} / { route_args} to file {file_path}.")

        if not os.path.exists(os.path.dirname(file_path)):
            os.makedirs(os.path.dirname(file_path))

        if isinstance(obj, str):
            json = obj
        else:
            json = orjson.dumps(obj)
        with open(file_path, "wb") as f:
            f.write(json)
