import json
import logging
import os
import string
from enum import Enum
from functools import cache
from pathlib import Path

import aiofile
import orjson
from packaging.version import Version
from ..utils import async_and_sync
from aerovaldb.aerovaldb import AerovalDB, get_method, put_method
from aerovaldb.exceptions import FileDoesNotExist, UnusedArguments
from aerovaldb.types import AccessType

logger = logging.getLogger(__name__)


class DataVersionMismatch(Exception):
    pass


class PyaerocomVersionToImplementationMapper:
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

    def __call__(self, *args, version: Version, **kwargs) -> str:
        if self.min_version is not None and version < self.min_version:
            raise DataVersionMismatch
        if self.max_version is not None and version > self.max_version:
            raise DataVersionMismatch

        return self.template.format(**kwargs)


class AerovalJsonFileDB(AerovalDB):
    def __init__(self, basedir: str | Path):
        self._basedir = basedir
        if isinstance(self._basedir, str):
            self._basedir = Path(self._basedir)

        self.PATH_LOOKUP = {
            "/v0/glob_stats/{project}/{experiment}/{frequency}": [
                PyaerocomVersionToImplementationMapper(
                    "./{project}/{experiment}/hm/glob_stats_{frequency}.json"
                )
            ],
            "/v0/contour/{project}/{experiment}/{obsvar}/{model}": [
                PyaerocomVersionToImplementationMapper(
                    "./{project}/{experiment}/contour/{obsvar}_{model}.geojson"
                )
            ],
            "/v0/ts/{project}/{experiment}/{location}/{network}/{obsvar}/{layer}": [
                PyaerocomVersionToImplementationMapper(
                    "./{project}/{experiment}/ts/{location}_{network}-{obsvar}_{layer}.json"
                )
            ],
            "/v0/experiments/{project}": [
                PyaerocomVersionToImplementationMapper("./{project}/experiments.json")
            ],
            "/v0/config/{project}/{experiment}": [
                PyaerocomVersionToImplementationMapper(
                    "./{project}/{experiment}/cfg_{project}_{experiment}.json"
                )
            ],
            "/v0/menu/{project}/{experiment}": [
                PyaerocomVersionToImplementationMapper(
                    "./{project}/{experiment}/menu.json"
                )
            ],
            "/v0/statistics/{project}/{experiment}": [
                PyaerocomVersionToImplementationMapper(
                    "./{project}/{experiment}/statistics.json"
                )
            ],
            "/v0/ranges/{project}/{experiment}": [
                PyaerocomVersionToImplementationMapper(
                    "./{project}/{experiment}/ranges.json"
                )
            ],
            "/v0/regions/{project}/{experiment}": [
                PyaerocomVersionToImplementationMapper(
                    "./{project}/{experiment}/regions.json"
                )
            ],
            "/v0/ts_weekly/{project}/{experiment}/{location}_{network}-{obsvar}_{layer}": [
                PyaerocomVersionToImplementationMapper(
                    "./{project}/{experiment}/ts/diurnal/{location}_{network}-{obsvar}_{layer}.json"
                )
            ],
            "/v0/profiles/{project}/{experiment}/{location}/{network}/{obsvar}": [
                PyaerocomVersionToImplementationMapper(
                    "./{project}/{experiment}/profiles/{location}_{network}-{obsvar}.json"
                )
            ],
            "/v0/forecast/{project}/{experiment}/{region}/{network}/{obsvar}/{layer}": [
                PyaerocomVersionToImplementationMapper(
                    "./{project}/{experiment}/forecast/{region}_{network}-{obsvar}_{layer}.json"
                )
            ],
            "/v0/gridded_map/{project}/{experiment}/{obsvar}/{model}": [
                PyaerocomVersionToImplementationMapper(
                    "./{project}/{experiment}/contour/{obsvar}_{model}.json"
                )
            ],
            "/v0/report/{project}/{experiment}/{title}": [
                PyaerocomVersionToImplementationMapper(
                    "./reports/{project}/{experiment}/{title}.json"
                )
            ],
            "/v0/map/{project}/{experiment}/{network}/{obsvar}/{layer}/{model}/{modvar}": [
                PyaerocomVersionToImplementationMapper(
                    "./{project}/{experiment}/map/{network}-{obsvar}_{layer}_{model}-{modvar}_{time}.json",
                    min_version="0.13.2",
                ),
                PyaerocomVersionToImplementationMapper(
                    "./{project}/{experiment}/map/{network}-{obsvar}_{layer}_{model}-{modvar}.json",
                    max_version="0.13.1",
                ),
            ],
        }
        # self.PATH_LOOKUP = {
        #    "/v0/model_style/{project}": [
        #        "./{project}/{experiment}/models-style.json",
        #        "./{project}/models-style.json",
        #    ],
        #    "/v0/map/{project}/{experiment}/{network}/{obsvar}/{layer}/{model}/{modvar}": [
        #        "./{project}/{experiment}/map/{network}-{obsvar}_{layer}_{model}-{modvar}_{time}.json",
        #        "./{project}/{experiment}/map/{network}-{obsvar}_{layer}_{model}-{modvar}.json",
        #    ],
        #    "/v0/scat/{project}/{experiment}/{network}-{obsvar}_{layer}_{model}-{modvar}": [
        #        "./{project}/{experiment}/scat/{network}-{obsvar}_{layer}_{model}-{modvar}_{time}.json",
        #        "./{project}/{experiment}/scat/{network}-{obsvar}_{layer}_{model}-{modvar}.json",
        #    ],
        #    "/v0/hm_ts/{project}/{experiment}": [
        #        "./{project}/{experiment}/hm/ts/{location}-{network}-{obsvar}-{layer}.json",
        #        "./{project}/{experiment}/hm/ts/{network}-{obsvar}-{layer}.json",
        #        "./{project}/{experiment}/hm/ts/stats_ts.json",
        #    ],
        # }

    @async_and_sync
    async def _get_version(self, project: str, experiment: str) -> Version:
        """
        Returns the version of pyaerocom used to generate the files for a given project
        and experiment.

        :param project : Project ID.
        :param experiment : Experiment ID.

        :return : A Version object.
        """
        file_path = str(
            self._basedir
        ) + "/{project}/{experiment}/cfg_{project}_{experiment}.json".format(
            project=project, experiment=experiment
        )

        async with aiofile.async_open(file_path, "r") as f:
            data = await f.read()

        data = orjson.loads(data)

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

    def _get_file_path_from_route(self, route, route_args, /, *args, **kwargs):
        file_path_templates: list[str] = self.PATH_LOOKUP.get(route, None)

        if file_path_templates is None:
            raise KeyError(f"No file path template found for route {route}.")

        substitutions = route_args | kwargs

        if not isinstance(file_path_templates, list):
            file_path_templates = [file_path_templates]

        relative_path = None
        for t in file_path_templates:
            fieldnames = [
                fname for _, fname, _, _ in string.Formatter().parse(t) if fname
            ]
            if set(fieldnames) != set(substitutions.keys()):
                logger.debug("A template was skipped due to superfluous arguments.")
                continue

            try:
                relative_path = t.format(**substitutions)
            except KeyError:
                continue

            break

        if relative_path is None:
            raise ValueError("Error in relative path resolution.")
        return Path(os.path.join(self._basedir, relative_path)).resolve()

    async def _get(self, route, route_args, *args, **kwargs):
        if len(args) > 0:
            raise UnusedArguments(
                f"Unexpected positional arguments {args}. Jsondb does not use additional positional arguments currently."
            )

        substitutions = route_args | kwargs

        relative_path = None
        for f in self.PATH_LOOKUP[route]:
            try:
                relative_path = f(
                    **substitutions,
                    version=await self._get_version(
                        route_args["project"], route_args["experiment"]
                    ),
                )
            except DataVersionMismatch:
                continue

            break

        if relative_path is None:
            raise Exception()

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
