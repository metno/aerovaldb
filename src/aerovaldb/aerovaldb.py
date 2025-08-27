import abc
import datetime
import functools
import inspect
from typing import Iterable

from aerovaldb.utils.query import QueryEntry

from .routes import Route
from .types import AccessType
from .utils import async_and_sync


def get_method(route):
    """Decorator for put functions, converts positional-only arguments into route_args

    :param route: the route for this method
    :param wrapped: function template, the wrapper function will never be called
    :returns: getter function
    """

    def wrap(wrapped):
        @functools.wraps(wrapped)
        async def wrapper(self, *args, **kwargs):
            sig = inspect.signature(wrapped)
            route_args = {}
            for pos, par in enumerate(sig.parameters):
                if pos == 0:
                    # first time, "self", skip
                    continue
                if sig.parameters[par].kind == inspect.Parameter.POSITIONAL_ONLY:
                    try:
                        route_args[par] = args[0]
                        args = args[1:]
                    except IndexError as iex:
                        raise IndexError(
                            f"{wrapped.__name__} got less parameters as expected (>= {len(route_args)+2}): {iex}"
                        )
            if len(args) > 0:
                raise IndexError(f"{len(args)} superfluous positional args provided.")
            return await self._get(
                route,
                route_args,
                **kwargs,
            )

        return wrapper

    return wrap


def put_method(route):
    """Decorator for put functions, converts positional-only arguments into route_args

    :param route: the route for this method
    :param wrapped: function template, the function will never be called
    :returns: putter function
    """

    def wrap(wrapped):
        @functools.wraps(wrapped)
        async def wrapper(self, obj, *args, **kwargs):
            sig = inspect.signature(wrapped)
            route_args = {}
            for pos, par in enumerate(sig.parameters):
                if pos <= 1:
                    # first time, "self", "obj" skip
                    continue
                if sig.parameters[par].kind == inspect.Parameter.POSITIONAL_ONLY:
                    try:
                        route_args[par] = args[0]
                        args = args[1:]
                    except IndexError as iex:
                        raise IndexError(
                            f"{wrapped.__name__} got less parameters as expected (>= {len(route_args)+2}): {iex}"
                        )
            if len(args) > 0:
                raise IndexError(f"{len(args)} superfluous positional args provided.")
            return await self._put(
                obj,
                route,
                route_args,
                **kwargs,
            )

        return wrapper

    return wrap


class AerovalDB(abc.ABC):
    def __init__(self):
        """AerovalDB is the base class for databases. Please make sure to initialize
        any instance of aerovaldb as a context manager to ensure resources to be release,
        e.g. database-handles.
        """
        pass

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        pass

    async def _get(self, route: Route, route_args: dict[str, str], **kwargs):
        """Abstract implementation of the main getter functions. All get and put
        functions map to this function, with a corresponding route as key
        to enable key/value pair put and get functionality.

        :param route: a route similar to a REST route
        :param route_args: route parameters, like format-dicts
        """
        raise NotImplementedError

    async def _put(self, obj, route: Route, route_args: dict[str, str], **kwargs):
        """Abstract implementation of the main getter functions. All get and put
        functions map to this function, with a corresponding route as key
        to enable key/value pair put and get functionality.

        :param obj: the object to put
        :param route: a route similar to a REST route
        :param route_args: route parameters, like format-dicts
        """
        raise NotImplementedError

    @async_and_sync
    @get_method(Route.GLOB_STATS)
    async def get_glob_stats(
        self,
        project: str,
        experiment: str,
        frequency: str,
        /,
        *args,
        access_type: str | AccessType = AccessType.OBJ,
        cache: bool = False,
        default=None,
        **kwargs,
    ):
        """Fetches a glob_stats object from the database.

        :param project: The project ID.
        :param experiment: The experiment ID.
        :param frequency: The frequency (eg. 'monthly')
        :param access_type: How the data is to be retrieved (See AccessType for details)
        :param cache: Whether to use cache for this read.
        :param default: Default value that will be returned instead of raising FileNotFoundError
        if not data was found (Will be returned as is and not converted to match access_type).

        :returns: The fetched data.
        """
        raise NotImplementedError

    @async_and_sync
    @get_method(Route.REGIONAL_STATS)
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
        access_type: str | AccessType = AccessType.OBJ,
        cache: bool = False,
        default=None,
        **kwargs,
    ):
        """Fetches regional stats from the database.

        :param project: The project ID.
        :param experiment: The experiment ID.
        :param frequency: The frequency.
        :param network: Observation network.
        :param variable: Variable name.
        """
        raise NotImplementedError

    @async_and_sync
    @get_method(Route.HEATMAP)
    async def get_heatmap(
        self,
        project: str,
        experiment: str,
        frequency: str,
        region: str,
        time: str,
        /,
        *args,
        access_type: str | AccessType = AccessType.OBJ,
        cache: bool = False,
        default=None,
        **kwargs,
    ):
        """Fetches heatmap data from the database

        :param project: The project ID.
        :param experiment: The experiment ID.
        :param frequency: The frequency.
        :param region: Region.
        :param time: Time.

        :param access_type: How the data is to be retrieved (See AccessType for details)
        :param cache: Whether to use cache for this read.
        :param default: Default value that will be returned instead of raising FileNotFoundError
        if not data was found (Will be returned as is and not converted to match access_type).

        :returns: The fetched data.
        """
        raise NotImplementedError

    @async_and_sync
    @put_method(Route.GLOB_STATS)
    async def put_glob_stats(
        self, obj, project: str, experiment: str, frequency: str, /, *args, **kwargs
    ):
        """Saves a glob_stats object to the database.

        :param obj: The object to be stored.
        :param project: The project ID.
        :param experiment: The experiment ID.
        :param frequency: The frequency (eg. 'monthly')
        """
        raise NotImplementedError

    @async_and_sync
    @get_method(Route.CONTOUR)
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
        """Fetch a contour object from the db.

        :param project: Project ID.
        :param experiment: Experiment ID.
        :param obsvar: Observation variable.
        :param model: Model ID.
        :param timestep: Optional timestep. Timestep will be required in the future.

        :param access_type: How the data is to be retrieved (See AccessType for details)
        :param cache: Whether to use cache for this read.
        :param default: Default value that will be returned instead of raising FileNotFoundError
            if no data was found (Will be returned as is and not converted to match access_type).

        :returns: The fetched data.
        """
        raise NotImplementedError

    @async_and_sync
    @put_method(Route.CONTOUR)
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
        """Put a contour object in the db.

        :param obj: The object to be stored.
        :param project: Project ID.
        :param experiment: Experiment ID.
        :param obsvar: Observation variable.
        :param model: Model ID.
        :param timestep: Optional timestep. Will be required in the future.
        """
        raise NotImplementedError

    @async_and_sync
    @get_method(Route.TIMESERIES)
    async def get_timeseries(
        self,
        project: str,
        experiment: str,
        location: str,
        network: str,
        obsvar: str,
        layer: str,
        /,
        *args,
        access_type: str | AccessType = AccessType.OBJ,
        cache: bool = False,
        default=None,
        **kwargs,
    ):
        """Fetches a timeseries from the db.

        :param project: Project id.
        :param experiment: Experiment ID.
        :param location: Location.
        :param network: Observation network.
        :param obsvar: Observation variable.
        :param layer: Layer.

        :param access_type: How the data is to be retrieved (See AccessType for details)
        :param cache: Whether to use cache for this read.
        :param default: Default value that will be returned instead of raising FileNotFoundError
            if no data was found (Will be returned as is and not converted to match access_type).

        :returns: The fetched data.
        """
        raise NotImplementedError

    @async_and_sync
    @put_method(Route.TIMESERIES)
    async def put_timeseries(
        self,
        obj,
        project: str,
        experiment: str,
        location: str,
        network: str,
        obsvar: str,
        layer: str,
        /,
        *args,
        **kwargs,
    ):
        """Places a timeseries in the db

        :param obj: The object to write into the db.
        :param project: Project ID
        :param experiment: Experiment ID.
        :param location: Location.
        :param network: Observation network.
        :param obsvar: Observation variable.
        :param layer: Layer.
        """
        raise NotImplementedError

    @async_and_sync
    async def list_glob_stats(
        self,
        project: str,
        experiment: str,
        /,
        access_type: str | AccessType = AccessType.URI,
    ) -> list[str]:
        """Lists the URI for each glob_stats object.

        :param project: str
        :param experiment: str

        :returns: List of URIs.
        """
        raise NotImplementedError

    @async_and_sync
    async def list_timeseries(
        self,
        project: str,
        experiment: str,
    ) -> list[QueryEntry]:
        """Returns a list of URIs of all timeseries files for
        a given project and experiment id.

        :param project: Project ID.
        :param experiment: Experiment ID.
        """
        raise NotImplementedError

    @async_and_sync
    @get_method(Route.TIMESERIES_WEEKLY)
    async def get_timeseries_weekly(
        self,
        project: str,
        experiment: str,
        location: str,
        network: str,
        obsvar: str,
        layer: str,
        /,
        *args,
        access_type: str | AccessType = AccessType.OBJ,
        cache: bool = False,
        default=None,
        **kwargs,
    ):
        """Fetches a weekly time series from the db.

        :param project: Project ID.
        :param experiment: Experiment ID.
        :param location: Location.
        :param network: Observation network.
        :param obsvar: Observation variable.
        :param layer: Layer.

        :param access_type: How the data is to be retrieved (See AccessType for details)
        :param cache: Whether to use cache for this read.
        :param default: Default value that will be returned instead of raising FileNotFoundError
        if not data was found (Will be returned as is and not converted to match access_type).

        :returns: The fetched data.
        """
        raise NotImplementedError

    @async_and_sync
    @put_method(Route.TIMESERIES_WEEKLY)
    async def put_timeseries_weekly(
        self,
        obj,
        project: str,
        experiment: str,
        location: str,
        network: str,
        obsvar: str,
        layer: str,
        /,
        *args,
        **kwargs,
    ):
        """Stores a weekly time series in the db.

        :param obj: The object to be stored.
        :param project: Project ID.
        :param experiment: Experiment ID.
        :param location: Location.
        :param network: Observation network.
        :param obsvar: Observation variable.
        :param layer: Layer.
        """
        raise NotImplementedError

    @async_and_sync
    @get_method(Route.EXPERIMENTS)
    async def get_experiments(
        self,
        project: str,
        /,
        *args,
        access_type: str | AccessType = AccessType.OBJ,
        cache: bool = False,
        default=None,
        **kwargs,
    ):
        """Fetches a list of experiments for a project from the db.

        :param project: Project ID.

        :param access_type: How the data is to be retrieved (See AccessType for details)
        :param cache: Whether to use cache for this read.
        :param default: Default value that will be returned instead of raising FileNotFoundError
        if not data was found (Will be returned as is and not converted to match access_type).

        :returns: The fetched data.
        """
        raise NotImplementedError

    @async_and_sync
    @put_method(Route.EXPERIMENTS)
    async def put_experiments(self, obj, project: str, /, *args, **kwargs):
        """Stores a list of experiments for a project to the db.

        :param project: Project ID.
        """
        raise NotImplementedError

    def rm_experiment_data(self, project: str, experiment: str):
        """Deletes ALL data associated with an experiment.

        :param project: Project ID.
        :param experiment: Experiment ID.
        """
        raise NotImplementedError

    def _list_experiments(
        self, project: str, /, has_results: bool = False
    ) -> list[str]:
        raise NotImplementedError

    @async_and_sync
    @get_method(Route.CONFIG)
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
        """Fetches a configuration from the db.

        :param project: Project ID.
        :param experiment: Experiment ID.

        :param access_type: How the data is to be retrieved (See AccessType for details)
        :param cache: Whether to use cache for this read.
        :param default: Default value that will be returned instead of raising FileNotFoundError
            if no data was found (Will be returned as is and not converted to match access_type).

        :returns: The fetched data.
        """
        raise NotImplementedError

    @async_and_sync
    @put_method(Route.CONFIG)
    async def put_config(self, obj, project: str, experiment: str, /, *args, **kwargs):
        """Stores a configuration to the db.

        :paran obj: The object to be stored.
        :param project: Project ID.
        :param experiment: Experiment ID.
        """

        raise NotImplementedError

    @async_and_sync
    @get_method(Route.MENU)
    async def get_menu(
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
        """Fetches a menu configuartion from the db.

        :param project: Project ID.
        :param experiment: Experiment ID.

        :param access_type: How the data is to be retrieved (See AccessType for details)
        :param cache: Whether to use cache for this read.
        :param default: Default value that will be returned instead of raising FileNotFoundError
        if not data was found (Will be returned as is and not converted to match access_type).

        :returns: The fetched data.
        """
        raise NotImplementedError

    @async_and_sync
    @put_method(Route.MENU)
    async def put_menu(self, obj, project: str, experiment: str, /, *args, **kwargs):
        """Stores a menu configuration in the db.

        :param obj: The object to be stored.
        :param project: Project ID.
        :param experiment: Experiment ID.
        """
        raise NotImplementedError

    @async_and_sync
    @get_method(Route.STATISTICS)
    async def get_statistics(
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
        """Fetches statistics for an experiment.

        :param project: Project ID.
        :param experiment: Experiment ID.

        :param access_type: How the data is to be retrieved (See AccessType for details)
        :param cache: Whether to use cache for this read.
        :param default: Default value that will be returned instead of raising FileNotFoundError
            if not data was found (Will be returned as is and not converted to match access_type).

        :returns: The fetched data.
        """
        raise NotImplementedError

    @async_and_sync
    @put_method(Route.STATISTICS)
    async def put_statistics(
        self, obj, project: str, experiment: str, /, *args, **kwargs
    ):
        """Stores statistics to the db.

        :param obj: The object to be stored.
        :param project: Project ID.
        :param experiment: Experiment ID.
        """
        raise NotImplementedError

    @async_and_sync
    @get_method(Route.RANGES)
    async def get_ranges(
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
        """Fetches ranges from the db.

        :param project: Project ID.
        :param experiment: Experiment ID.

        :param access_type: How the data is to be retrieved (See AccessType for details)
        :param cache: Whether to use cache for this read.
        :param default: Default value that will be returned instead of raising FileNotFoundError
            if not data was found (Will be returned as is and not converted to match access_type).

        :returns: The fetched data.
        """
        raise NotImplementedError

    @async_and_sync
    @put_method(Route.RANGES)
    async def put_ranges(self, obj, project: str, experiment: str, /, *args, **kwargs):
        """Stores ranges in db.

        :param obj: The object to be stored.
        :param project: Project ID.
        :param experiment: Experiment ID.
        """
        raise NotImplementedError

    @async_and_sync
    @get_method(Route.REGIONS)
    async def get_regions(
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
        """Fetches regions from db.

        :param project: Project ID.
        :param experiment: Experiment ID.

        :param access_type: How the data is to be retrieved (See AccessType for details)
        :param cache: Whether to use cache for this read.
        :param default: Default value that will be returned instead of raising FileNotFoundError
            if not data was found (Will be returned as is and not converted to match access_type).

        :returns: The fetched data.
        """
        raise NotImplementedError

    @async_and_sync
    @put_method(Route.REGIONS)
    async def put_regions(self, obj, project: str, experiment: str, /, *args, **kwargs):
        """Stores regions in db.

        :param obj: Object to be stored.
        :param project: Project ID.
        :param experiment: Experiment ID.
        """
        raise NotImplementedError

    @async_and_sync
    @get_method(Route.MODELS_STYLE)
    async def get_models_style(
        self,
        project: str,
        /,
        experiment: str | None = None,
        *args,
        access_type: str | AccessType = AccessType.OBJ,
        cache: bool = False,
        default=None,
        **kwargs,
    ):
        """Fetches model styles from db.

        :param project: Project ID.
        :param experiment (Optional): Experiment ID.

        :param access_type: How the data is to be retrieved (See AccessType for details)
        :param cache: Whether to use cache for this read.
        :param default: Default value that will be returned instead of raising FileNotFoundError
            if not data was found (Will be returned as is and not converted to match access_type).

        :returns: The fetched data.
        """
        raise NotImplementedError

    @async_and_sync
    @put_method(Route.MODELS_STYLE)
    async def put_models_style(
        self, obj, project: str, /, experiment: str | None = None, *args, **kwargs
    ):
        """Stores model styles config in db.

        :param obj: Object to be stored.
        :param project: Project ID.
        :param experiment (Optional): Experiment ID.
        """
        raise NotImplementedError

    @async_and_sync
    @get_method(Route.MAP)
    async def get_map(
        self,
        project: str,
        experiment: str,
        network: str,
        obsvar: str,
        layer: str,
        model: str,
        modvar: str,
        time: str,
        /,
        *args,
        frequency: str | None = None,
        season: str | None = None,
        access_type: str | AccessType = AccessType.OBJ,
        cache: bool = False,
        default=None,
        **kwargs,
    ):
        """Fetches map data from db.

        :param project: Project Id
        :param experiment: Experiment ID.
        :param network: Observation network
        :param obsvar: Observation variable.
        :param layer: Layer
        :param model: Model ID
        :param modvar: Model variable.
        :param time: Time parameter.
        :param frequency: Optional frequency (eg. 'monthly')
        :param season: Optional season.

        :param access_type: How the data is to be retrieved (See AccessType for details)
        :param cache: Whether to use cache for this read.
        :param default: Default value that will be returned instead of raising FileNotFoundError
            if not data was found (Will be returned as is and not converted to match access_type).

        :returns: The fetched data.

        Note
        ----
        If either frequency or season are provided, they both must be provided.
        """
        raise NotImplementedError

    @async_and_sync
    @put_method(Route.MAP)
    async def put_map(
        self,
        obj,
        project: str,
        experiment: str,
        network: str,
        obsvar: str,
        layer: str,
        model: str,
        modvar: str,
        time: str,
        /,
        *args,
        **kwargs,
    ):
        """Stores map data in db.

        :param obj: The Object to be stored.
        :param project: Project Id
        :param experiment: Experiment ID.
        :param network: Observation network
        :param obsvar: Observation variable.
        :param layer: Layer
        :param model: Model ID
        :param modvar: Model variable.
        :param time: Time parameter.
        """
        raise NotImplementedError

    @async_and_sync
    async def list_map(
        self,
        project: str,
        experiment: str,
    ) -> list[str]:
        """Lists all map files for a given project / experiment combination.

        :param project: The project ID.
        :param experiment: The experiment ID.

        :returns: List with the URIs.
        """
        raise NotImplementedError

    @async_and_sync
    @get_method(Route.SCATTER)
    async def get_scatter(
        self,
        project: str,
        experiment: str,
        network: str,
        obsvar: str,
        layer: str,
        model: str,
        modvar: str,
        time: str,
        /,
        *args,
        access_type: str | AccessType = AccessType.OBJ,
        cache: bool = False,
        default=None,
        **kwargs,
    ):
        """Get scat.

        :param project: Project ID.
        :param experiment: Experiment ID.
        :param network: Observation network.
        :param obsvar: Observation variable.
        :param layer: Layer.
        :param model: Model ID.
        :param modvar: Model variable.
        :param time: Time parameter.

        :param access_type: How the data is to be retrieved (See AccessType for details)
        :param cache: Whether to use cache for this read.
        :param default: Default value that will be returned instead of raising FileNotFoundError
            if not data was found (Will be returned as is and not converted to match access_type).

        :returns: The fetched data.
        """
        raise NotImplementedError

    @async_and_sync
    @put_method(Route.SCATTER)
    async def put_scatter(
        self,
        obj,
        project: str,
        experiment: str,
        network: str,
        obsvar: str,
        layer: str,
        model: str,
        modvar: str,
        time: str,
        /,
        *args,
        **kwargs,
    ):
        """Stores scat in db.

        :param obj: Object to be stored.
        :param project: Project ID.
        :param experiment: Experiment ID.
        :param network: Observation network.
        :param obsvar: Observation variable.
        :param layer: Layer.
        :param model: Model ID.
        :param modvar: Model variable.
        :param time: Time paramter.
        """
        raise NotImplementedError

    @async_and_sync
    @get_method(Route.PROFILES)
    async def get_profiles(
        self,
        project: str,
        experiment: str,
        location: str,
        network: str,
        obsvar: str,
        /,
        *args,
        access_type: str | AccessType = AccessType.OBJ,
        cache: bool = False,
        default=None,
        **kwargs,
    ):
        """Fetches profiles from db.

        :param project: Project ID.
        :param experiment: Experiment ID.
        :param location: Location.
        :param network: Observation network.
        :param obsvar: Observation variable.

        :param access_type: How the data is to be retrieved (See AccessType for details)
        :param cache: Whether to use cache for this read.
        :param default: Default value that will be returned instead of raising FileNotFoundError
            if not data was found (Will be returned as is and not converted to match access_type).

        :returns: The fetched data.
        """
        raise NotImplementedError

    @async_and_sync
    @put_method(Route.PROFILES)
    async def put_profiles(
        self,
        obj,
        project: str,
        experiment: str,
        location: str,
        network: str,
        obsvar: str,
        /,
        *args,
        **kwargs,
    ):
        """Stores profiles in db.

        :param obj: Object to be stored.
        :param project: Project ID._
        :param experiment: Experiment ID.
        :param location: Location.
        :param network: Observation network.
        :param obsvar: Observation variable.
        """
        raise NotImplementedError

    @async_and_sync
    @get_method(Route.HEATMAP_TIMESERIES)
    async def get_heatmap_timeseries(
        self,
        project: str,
        experiment: str,
        region: str,
        network: str,
        obsvar: str,
        layer: str,
        /,
        *args,
        access_type: str | AccessType = AccessType.OBJ,
        cache: bool = False,
        default=None,
        **kwargs,
    ):
        """Fetches heatmap timeseries.

        :param project: Project ID.
        :param experiment: Experiment ID.
        :param region: Region ID.
        :param network: Observation Network.
        :param obsvar: Observation variable.
        :param layer: Layer.

        :param access_type: How the data is to be retrieved (See AccessType for details)
        :param cache: Whether to use cache for this read.
        :param default: Default value that will be returned instead of raising FileNotFoundError
            if not data was found (Will be returned as is and not converted to match access_type).

        :returns: The fetched data.
        """
        raise NotImplementedError

    @async_and_sync
    @put_method(Route.HEATMAP_TIMESERIES)
    async def put_heatmap_timeseries(
        self,
        obj,
        project: str,
        experiment: str,
        region: str,
        network: str,
        obsvar: str,
        layer: str,
        /,
        *args,
        **kwargs,
    ):
        """Stores heatmap timeseries.

        :param obj: The object to be stored.
        :param project: Project ID.
        :param experiment: Experiment ID.
        :param region: Region ID.
        :param network: Observation Network.
        :param obsvar: Observation variable.
        :param layer: Layer.
        """
        raise NotImplementedError

    @async_and_sync
    @get_method(Route.FORECAST)
    async def get_forecast(
        self,
        project: str,
        experiment: str,
        region: str,
        network: str,
        obsvar: str,
        layer: str,
        /,
        *args,
        access_type: str | AccessType = AccessType.OBJ,
        cache: bool = False,
        default=None,
        **kwargs,
    ):
        """Fetch forecast.

        :param project: Project ID.
        :param experiment: Experiment ID.
        :param region: Region ID.
        :param network: Observation Network.
        :param obsvar: Observation variable.
        :param layer: Layer.

        :param access_type: How the data is to be retrieved (See AccessType for details)
        :param cache: Whether to use cache for this read.
        :param default: Default value that will be returned instead of raising FileNotFoundError
            if not data was found (Will be returned as is and not converted to match access_type).

        :returns: The fetched data.
        """
        raise NotImplementedError

    @async_and_sync
    @get_method(Route.FAIRMODE)
    async def get_fairmode(
        self,
        project: str,
        experiment: str,
        region: str,
        network: str,
        obsvar: str,
        layer: str,
        model: str,
        time: str,
        /,
        *args,
        access_type: str | AccessType = AccessType.OBJ,
        cache: bool = False,
        default=None,
        **kwargs,
    ):
        """Fetch fairmode.

        :param project: Project ID.
        :param experiment: Experiment ID.
        :param region: Region ID.
        :param network: Observation Network.
        :param obsvar: Observation variable.
        :param layer: Layer.

        :param access_type: How the data is to be retrieved (See AccessType for details)
        :param cache: Whether to use cache for this read.
        :param default: Default value that will be returned instead of raising FileNotFoundError
            if not data was found (Will be returned as is and not converted to match access_type).

        :returns: The fetched data.
        """
        raise NotImplementedError

    @async_and_sync
    @put_method(Route.FORECAST)
    async def put_forecast(
        self,
        obj,
        project: str,
        experiment: str,
        region: str,
        network: str,
        obsvar: str,
        layer: str,
        /,
        *args,
        **kwargs,
    ):
        """Store forecast.

        :param obj: The Object to be stored.
        :param project: Project ID.
        :param experiment: Experiment ID.
        :param region: Region ID.
        :param network: Observation Network.
        :param obsvar: Observation variable.
        :param layer: Layer.
        """
        raise NotImplementedError

    @async_and_sync
    @put_method(Route.FAIRMODE)
    async def put_fairmode(
        self,
        obj,
        project: str,
        experiment: str,
        region: str,
        network: str,
        obsvar: str,
        layer: str,
        model: str,
        time: str,
        /,
        *args,
        **kwargs,
    ):
        """Store fairmode.

        :param obj: The Object to be stored.
        :param project: Project ID.
        :param experiment: Experiment ID.
        :param region: Region ID.
        :param network: Observation Network.
        :param obsvar: Observation variable.
        :param layer: Layer.
        """
        raise NotImplementedError

    @async_and_sync
    @get_method(Route.GRIDDED_MAP)
    async def get_gridded_map(
        self,
        project: str,
        experiment: str,
        obsvar: str,
        model: str,
        /,
        *args,
        access_type: str | AccessType = AccessType.OBJ,
        cache: bool = False,
        default=None,
        **kwargs,
    ):
        """Fetches gridded map.

        :param project: Project ID.
        :param experiment: Experiment ID.
        :param obsvar: Observation variable.
        :param model: Model ID.

        :param access_type: How the data is to be retrieved (See AccessType for details)
        :param cache: Whether to use cache for this read.
        :param default: Default value that will be returned instead of raising FileNotFoundError
            if not data was found (Will be returned as is and not converted to match access_type).

        :returns: The fetched data.
        """
        raise NotImplementedError

    @async_and_sync
    @put_method(Route.GRIDDED_MAP)
    async def put_gridded_map(
        self,
        obj,
        project: str,
        experiment: str,
        obsvar: str,
        model: str,
        /,
        *args,
        **kwargs,
    ):
        """Store gridded map.

        :param obj: The object to be stored.
        :param project: Project ID.
        :param experiment: Experiment ID.
        :param obsvar: Observation variable.
        :param model: Model ID.
        """
        raise NotImplementedError

    @async_and_sync
    @get_method(Route.REPORT)
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
        """Fetch report.

        :param project: Project ID.
        :param experiment: Experiment ID.
        :param title: Report title (ie. filename without extension).

        :param access_type: How the data is to be retrieved (See AccessType for details)
        :param cache: Whether to use cache for this read.
        :param default: Default value that will be returned instead of raising FileNotFoundError
            if not data was found (Will be returned as is and not converted to match access_type).

        :returns: The fetched data.
        """
        raise NotImplementedError

    @async_and_sync
    @put_method(Route.REPORT)
    async def put_report(
        self, obj, project: str, experiment: str, title: str, /, *args, **kwargs
    ):
        """Store report.

        :param obj: The object to be stored.
        :param project: Project ID.
        :param experiment: Experiment ID.
        :param title: Report title (ie. filename without extension).
        """
        raise NotImplementedError

    @async_and_sync
    async def get_by_uri(
        self,
        uri: str | QueryEntry,
        /,
        access_type: str | AccessType,
        cache: bool = False,
        default=None,
    ):
        """Gets a stored object by its URI.

        :param uri: URI of the item to fetch.
        :param access_type: See AccessType.
        :param cache: Whether to use the cache.
        :param default: If provided, this value will be returned instead of raising
            a FileNotFoundError if not file exists. The provided object will be returned
            as is, and will not be converted to match access_type.

        Note:
        -----
        URI is intended to be consistent between implementations but not between
        versions of aerovaldb. Using :meth:`aerovaldb.AerovalDB.get_by_uri`
        to fetch an identifier which can then be written to another connector using
        its respective :meth:`aerovaldb.AerovalDB.put_by_uri` method is a supported
        use case.
        """
        raise NotImplementedError

    @async_and_sync
    async def put_by_uri(self, obj, uri: str | QueryEntry):
        """Replaces a stored object by uri with a new object.

        :param obj: The object to be stored. Either a json str, or a
        json serializable python object.
        :param uri: The uri as which to store the object.

        Note:
        -----
        URI is intended to be consistent between implementations but not between
        versions of aerovaldb. Using :meth:`aerovaldb.AerovalDB.get_by_uri`
        to fetch an identifier which can then be written to another connector using
        its respective :meth:`aerovaldb.AerovalDB.put_by_uri` method is a supported
        use case.
        """
        raise NotImplementedError

    def lock(self):
        """Acquires an exclusive advisory lock to coordinate file access
        between instances of aerovaldb. Intended to be used as a context
        manager.

        See also: `Locking <https://aerovaldb.readthedocs.io/en/latest/locking.html>`_
        """
        raise NotImplementedError

    def _normalize_access_type(
        self, access_type: AccessType | str | None, default: AccessType = AccessType.OBJ
    ) -> AccessType:
        """Normalizes the access_type to an instance of AccessType enum.

        :param access_type: AccessType instance or string convertible to AccessType
        :param default: The type to return if access_type is None. Defaults to AccessType.OBJ
        :raises ValueError: If str access_type can't be converted to AccessType.
        :raises ValueError: If access_type is not str or AccessType

        :returns: The normalized AccessType.
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

        assert False

    @async_and_sync
    async def list_all(self, access_type: str | AccessType = AccessType.URI):
        """Returns a list of identifiers for each object stored in the database

        :param access_type: What to return (This is implementation specific, but
        in general each implementation should support URI, some may support FILE_PATH).
        :raises UnsupportedOperation:
            For non-supported acces types.
        """
        raise NotImplementedError

    @async_and_sync
    @get_method(Route.REPORT_IMAGE)
    async def get_report_image(
        self,
        project: str,
        experiment: str,
        path: str,
        access_type: str | AccessType = AccessType.BLOB,
    ):
        """
        Getter for static images that are referenced from the report json files.

        :param project: Project ID.
        :param experiment: Experiment ID.
        :param access_type: One of AccessType.BLOB, AccessType.FILE_PATH

        :returns: Either a string (If file path requested) or a bytes object with the
            image data
        """
        raise NotImplementedError

    @async_and_sync
    @put_method(Route.REPORT_IMAGE)
    async def put_report_image(self, obj, project: str, experiment: str, path: str):
        """
        Putter for static images that are referenced from the report json files.

        :param obj: A bytes object representing the image data to be written.
        :param project: Project ID.
        :param experiment: Experiment ID.

        :returns: Either a string (If file path requested) or a bytes object with the
            image data
        """
        raise NotImplementedError

    @async_and_sync
    @get_method(Route.MAP_OVERLAY)
    async def get_map_overlay(
        self,
        project: str,
        experiment: str,
        source: str,
        variable: str,
        date: str,
        access_type: str | AccessType = AccessType.BLOB,
    ):
        """Getter for map overlay images.

        :param project: Project ID.
        :param experiment: Experiment ID.
        :param source: Data source. Can be either an observation network or a model ID.
        :param variable: Variable name.
        :param date: Date.
        """
        raise NotImplementedError

    @async_and_sync
    @put_method(Route.MAP_OVERLAY)
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

        :param obj: The object to be stored.
        :param project: Project ID.
        :param experiment: Experiment ID.
        :param source: Data source. Can be either an observation network or a model ID.
        :param variable: Variable name.
        :param date: Date.
        """
        raise NotImplementedError

    @async_and_sync
    async def get_experiment_mtime(
        self, project: str, experiment: str
    ) -> datetime.datetime:
        """
        :param project: Project ID.
        :param experiment: Experiment ID.
        """
        uri = await self.get_config(project, experiment, access_type=AccessType.URI)
        return await self.get_by_uri(uri, access_type=AccessType.MTIME)

    @async_and_sync
    async def query(
        self, asset_type: Route | Iterable[Route] | None = None, **kwargs
    ) -> list[QueryEntry]:
        """Query function for getting information about assets
        stored in the db.

        :param asset_type: Enum of the type of asset to query (Can be an iterable of multiple types). By default,
        all asset types will be included.
        :param kwargs: Optional additional filter arguments. Will be matched against QueryEntry.meta.
        All provided keys must match. For possible keys see function signature of the getter for which
        you want to match. For instance :meth:`aerovaldb.AerovalDB.get_experiments()` can be filtered on
        the project key.

        :return: A list of QueryEntry objects that contains the URI and information about
        the queried files.

        Example:
        >>> import tempfile
        >>> import aerovaldb
        >>>
        >>> with tempfile.TemporaryDirectory() as dir:
        ...     with aerovaldb.open(f"json_files:{dir}") as db:
        ...         db.put_experiments({}, "project1")
        ...         db.put_experiments({}, "project2")
        ...         db.query(aerovaldb.Route.EXPERIMENTS, project="project1")[0].uri
        ...         db.query(aerovaldb.Route.EXPERIMENTS, project="project1")[0].meta
        '/v0/experiments/project1?version=0.0.1'
        {'project': 'project1'}
        """
        raise NotImplementedError

    @async_and_sync
    async def rm_by_uri(self, uri: str):
        """Removes the asset associated with the provided uri. The provided
        uri must match a whole file (ie. uris corresponding to filtered uris
        will not work and will raise an UnsupporedOperationError).

        :param uri: URI to remove.
        :raises UnsupporedOperation: If URI corresponds to a filtered endpoint.
        """
        raise NotImplementedError
