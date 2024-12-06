import logging
from abc import ABC
from typing import Awaitable, Callable, Mapping

from packaging.version import Version

logger = logging.getLogger(__name__)

VersionProvider = Callable[[str, str], Awaitable[Version]]


class SkipMapper(Exception):
    """
    Exception raised when a TemplateMapper does not want to or
    can't handle a request.
    """

    pass


class StringMapper:
    """
    Class for mapping one type of string to the appropriate other
    type of string. It is used by jsonfiledb to map from route to
    the appropriate template string, and in sqlitedb to map from
    route to the correct table name.

    It supports delivering different value strings based on
    additional constraints (such as version).
    """

    def __init__(self, lookup_table: dict, /, version_provider: VersionProvider):
        """
        :param lookup_table : A configuration lookuptable.
        """
        self._lookuptable = lookup_table
        for k in self._lookuptable:
            # A single string will always be returned for that key.
            if isinstance(self._lookuptable[k], str):
                self._lookuptable[k] = ConstantMapper(self._lookuptable[k])

            # Make sure value is a list.
            if not isinstance(self._lookuptable[k], list):
                self._lookuptable[k] = [self._lookuptable[k]]

            # If stringlist of len > 1, treat it as a priority order.
            if len(self._lookuptable[k]) > 1 and all(
                [isinstance(x, str) for x in self._lookuptable[k]]
            ):
                self._lookuptable[k] = [PriorityMapper(self._lookuptable[k])]

        self._version_provider = version_provider

    def __iter__(self):
        return iter(self._lookuptable.keys())

    async def lookup(self, key: str, **kwargs) -> str:
        """
        Performs a lookup of the value for the given key.

        :param key : The key for which to lookup a value.
        :param kwargs : Additional values which will be used for constraint
        matching.
        :raises KeyError
            If no entry exists for the key in the lookup table.
        :return
            The looked up string value.
        """
        if (version := kwargs.pop("version", None)) is None:
            version_provider = self._version_provider
        else:

            async def version_helper(p, e):
                return Version(version)

            version_provider = version_helper

        try:
            values = self._lookuptable[key]
        except KeyError as e:
            raise KeyError(f"Key '{key}' does not exist in lookup table.") from e

        kwargs = kwargs | {"version_provider": version_provider}
        return_value = None
        for v in values:
            try:
                return_value = await v(**kwargs)
            except SkipMapper:
                continue

            break

        if not return_value:
            raise ValueError(f"No valid value found for key '{key}'")

        return return_value


class Mapper(ABC):
    """
    This class is a base class for objects that implement a
    file path template selection algorithm. Inheriting
    implementations should implement the __call_ function,
    and raising SkipMapper if the implementation can't or
    won't handle the request.
    """

    async def __call__(self, *args, **kwargs) -> str:
        raise NotImplementedError


class VersionConstraintMapper(Mapper):
    """
    This class returns its provided template if the data version read
    from a config file matches the configured bounds of this class.

    Version is matched according to the following equation:
        min_version <= version < max_version
    """

    def __init__(
        self,
        template: str,
        *,
        min_version: str | None = None,
        max_version: str | None = None,
    ):
        """
        :param template : The template string to return.
        :min_version : The minimum version to which to apply this template (inclusive).
        :max_version : The maximum version to which to apply this template (exclusive).
        :version_provider :
            Function or other callable that takes 'project' and 'experiment', returning
            the Pyaerocom version that wrote the data to be read.
        """
        self.min_version = None
        self.max_version = None

        if min_version is not None:
            self.min_version = Version(min_version)
        if max_version is not None:
            self.max_version = Version(max_version)

        self.template = template

    async def __call__(self, *args, **kwargs) -> str:
        version_provider = kwargs.pop("version_provider")
        version = await version_provider(kwargs["project"], kwargs["experiment"])
        if not version:
            raise ValueError("No version provided")
        logger.debug(f"Trying template string {self.template}")

        if self.min_version is not None and version < self.min_version:
            logging.debug(
                f"Skipping due to version mismatch. {version} < {self.min_version}"
            )
            raise SkipMapper
        if self.max_version is not None and version >= self.max_version:
            logging.debug(
                f"Skipping due to version mismatch. {version} >= {self.max_version}"
            )
            raise SkipMapper

        return self.template


class PriorityMapper(Mapper):
    """
    This class takes a list of templates, trying them in turn
    and returning the first template that fits the provided
    parameters.
    """

    def __init__(self, templates: list[str] | dict):
        """
        :param templates
            If list, a list of template strings which will
            be matched in order.
            If dict, a mapping between the template string
            and an alternative string to match against is
            expected (i. e. {"test", "{a}"} would try matching
            against "{a}" during lookup, but return "test" if
            match.)
        """
        if isinstance(templates, list):
            self.templates = templates
            self.match = templates
        elif isinstance(templates, dict):
            self.templates = []
            self.match = []
            for k, v in templates.items():
                self.templates.append(k)
                self.match.append(v)

    async def __call__(self, *args, **kwargs) -> str:
        selected_template = None
        for t, m in zip(self.templates, self.match):
            try:
                m.format(**kwargs)
                selected_template = t
                break
            except:
                continue

        if selected_template is None:
            raise SkipMapper

        return selected_template


class ConstantMapper(Mapper):
    def __init__(self, template: str):
        self.template = template

    async def __call__(self, *args, **kwargs) -> str:
        return self.template
