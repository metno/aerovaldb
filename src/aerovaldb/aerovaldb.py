import abc
import functools
import inspect
from typing import Generator
from .types import AccessType
from .utils import async_and_sync
from .routes import *


def get_method(route):
    """Decorator for put functions, converts positional-only arguments into route_args

    :param route: the route for this method
    :param wrapped: function template, the wrapper function will never be called
    :return: getter function
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

            return await self._get(route, route_args, *args, cache=False, **kwargs)

        return wrapper

    return wrap


def put_method(route):
    """Decorator for put functions, converts positional-only arguments into route_args

    :param route: the route for this method
    :param wrapped: function template, the function will never be called
    :return: putter function
    """

    def wrap(wrapped):
        @functools.wraps(wrapped)
        async def wrapper(self, obj, *args, **kwargs):
            print(obj, args)
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

            return await self._put(obj, route, route_args, *args, **kwargs)

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

    async def _get(self, route: str, route_args: dict[str, str], *args, **kwargs):
        """Abstract implementation of the main getter functions. All get and put
        functions map to this function, with a corresponding route as key
        to enable key/value pair put and get functionality.

        :param route: a route similar to a REST route
        :param route_args: route parameters, like format-dicts
        """
        raise NotImplementedError

    async def _put(self, obj, route: str, route_args: dict[str, str], *args, **kwargs):
        """Abstract implementation of the main getter functions. All get and put
        functions map to this function, with a corresponding route as key
        to enable key/value pair put and get functionality.

        :param obj: the object to put
        :param route: a route similar to a REST route
        :param route_args: route parameters, like format-dicts
        """
        raise NotImplementedError

    @async_and_sync
    @get_method(ROUTE_GLOB_STATS)
    async def get_glob_stats(
        self, project: str, experiment: str, frequency: str, /, *args, **kwargs
    ):
        """Fetches a glob_stats object from the database.

        :param project: The project ID.
        :param experiment: The experiment ID.
        :param frequency: The frequency (eg. 'monthly')
        :param access_type: How the data is to be retrieved. One of "OBJ", "JSON_STR", "FILE_PATH"
            "OBJ" (Default) a python object with the data is returned.
            "JSON_STR" the raw json string is returned.
            "FILE_PATH" the path to the file where the data is stored is returned.
        """
        raise NotImplementedError

    @async_and_sync
    @get_method(ROUTE_REG_STATS)
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
        raise NotImplementedError

    @async_and_sync
    @get_method(ROUTE_HEATMAP)
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
        raise NotImplementedError

    @async_and_sync
    @put_method(ROUTE_GLOB_STATS)
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

    def list_glob_stats(
        self, project: str, experiment: str
    ) -> Generator[str, None, None]:
        """Generator that lists the uuid for each glob_stats object.

        :param project: str
        :param experiment: str

        :return Generator of UUIDs.
        """
        raise NotImplementedError

    @async_and_sync
    @get_method(ROUTE_CONTOUR)
    async def get_contour(
        self, project: str, experiment: str, obsvar: str, model: str, /, *args, **kwargs
    ):
        """Fetch a contour object from the db.

        :param project: Project ID.
        :param experiment: Experiment ID.
        :param obsvar: Observation variable.
        :param model: Model ID.
        :param access_type: How the data is to be retrieved. One of "OBJ", "JSON_STR", "FILE_PATH"
            "OBJ" (Default) a python object with the data is returned.
            "JSON_STR" the raw json string is returned.
            "FILE_PATH" the path to the file where the data is stored is returned.
        """
        raise NotImplementedError

    @async_and_sync
    @put_method(ROUTE_CONTOUR)
    async def put_contour(
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
        """Put a contour object in the db.

        :param obj: The object to be stored.
        :param project: Project ID.
        :param experiment: Experiment ID.
        :param obsvar: Observation variable.
        :param model: Model ID.
        """
        raise NotImplementedError

    @async_and_sync
    @get_method(ROUTE_TIMESERIES)
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
        **kwargs,
    ):
        """Fetches a timeseries from the db.

        :param project: Project id.
        :param experiment: Experiment ID.
        :param location: Location.
        :param network: Observation network.
        :param obsvar: Observation variable.
        :param layer: Layer.
        :param access_type: How the data is to be retrieved. One of "OBJ", "JSON_STR", "FILE_PATH"
            "OBJ" (Default) a python object with the data is returned.
            "JSON_STR" the raw json string is returned.
            "FILE_PATH" the path to the file where the data is stored is returned.
        """
        raise NotImplementedError

    @async_and_sync
    @put_method(ROUTE_TIMESERIES)
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
        :param access_type: How the data is to be retrieved. One of "OBJ", "JSON_STR", "FILE_PATH"
            "OBJ" (Default) a python object with the data is returned.
            "JSON_STR" the raw json string is returned.
            "FILE_PATH" the path to the file where the data is stored is returned.
        """
        raise NotImplementedError

    def list_timeseries(
        self, project: str, experiment: str
    ) -> Generator[str, None, None]:
        """Returns a list of uuids of all timeseries files for
        a given project and experiment id.

        :param project : Project ID.
        :param experiment : Experiment ID.
        """
        raise NotImplementedError

    @async_and_sync
    @get_method(ROUTE_TIMESERIES_WEEKLY)
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
        **kwargs,
    ):
        """Fetches a weekly time series from the db.

        :param project: Project ID.
        :param experiment: Experiment ID.
        :param location: Location.
        :param network: Observation network.
        :param obsvar: Observation variable.
        :param layer: Layer.
        """
        raise NotImplementedError

    @async_and_sync
    @put_method(ROUTE_TIMESERIES_WEEKLY)
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
    @get_method(ROUTE_EXPERIMENTS)
    async def get_experiments(self, project: str, /, *args, **kwargs):
        """Fetches a list of experiments for a project from the db.

        :param project: Project ID.
        """
        raise NotImplementedError

    def rm_experiment_data(self, project: str, experiment: str):
        """Deletes ALL data associated with an experiment.

        :param project : Project ID.
        :param experiment : Experiment ID.
        """
        raise NotImplementedError

    def _list_experiments(
        self, project: str, /, has_results: bool = False
    ) -> list[str]:
        raise NotImplementedError

    @async_and_sync
    @get_method(ROUTE_CONFIG)
    async def get_config(self, project: str, experiment: str, /, *args, **kwargs):
        """Fetches a configuration from the db.

        :param project: Project ID.
        :param experiment: Experiment ID.
        """
        raise NotImplementedError

    @async_and_sync
    @put_method(ROUTE_CONFIG)
    async def put_config(self, obj, project: str, experiment: str, /, *args, **kwargs):
        """Stores a configuration to the db.

        :paran obj: The object to be stored.
        :param project: Project ID.
        :param experiment: Experiment ID.
        """

        raise NotImplementedError

    @async_and_sync
    @get_method(ROUTE_MENU)
    async def get_menu(self, project: str, experiment: str, /, *args, **kwargs):
        """Fetches a menu configuartion from the db.

        :param project: Project ID.
        :param experiment: Experiment ID.
        """
        raise NotImplementedError

    @async_and_sync
    @put_method(ROUTE_MENU)
    async def put_menu(self, obj, project: str, experiment: str, /, *args, **kwargs):
        """Stores a menu configuration in the db.

        :param obj: The object to be stored.
        :param project: Project ID.
        :param experiment: Experiment ID.
        """
        raise NotImplementedError

    @async_and_sync
    @get_method(ROUTE_STATISTICS)
    async def get_statistics(self, project: str, experiment: str, /, *args, **kwargs):
        """Fetches statistics for an experiment.

        :param project: Project ID.
        :param experiment: Experiment ID.
        """
        raise NotImplementedError

    @async_and_sync
    @put_method(ROUTE_STATISTICS)
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
    @get_method(ROUTE_RANGES)
    async def get_ranges(self, project: str, experiment: str, /, *args, **kwargs):
        """Fetches ranges from the db.

        :param project: Project ID.
        :param experiment: Experiment ID.
        """
        raise NotImplementedError

    @async_and_sync
    @put_method(ROUTE_RANGES)
    async def put_ranges(self, obj, project: str, experiment: str, /, *args, **kwargs):
        """Stores ranges in db.

        :param obj: The object to be stored.
        :param project: Project ID.
        :param experiment: Experiment ID.
        """
        raise NotImplementedError

    @async_and_sync
    @get_method(ROUTE_REGIONS)
    async def get_regions(self, project: str, experiment: str, /, *args, **kwargs):
        """Fetches regions from db.

        :param project: Project ID.
        :param experiment: Experiment ID.
        """
        raise NotImplementedError

    @async_and_sync
    @put_method(ROUTE_REGIONS)
    async def put_regions(self, obj, project: str, experiment: str, /, *args, **kwargs):
        """Stores regions in db.

        :param obj: Object to be stored.
        :param project: Project ID.
        :param experiment: Experiment ID.
        """
        raise NotImplementedError

    @async_and_sync
    @get_method(ROUTE_MODELS_STYLE)
    async def get_models_style(
        self, project: str, /, experiment: str | None = None, *args, **kwargs
    ):
        """Fetches model styles from db.

        :param project: Project ID.
        :param experiment (Optional): Experiment ID.
        """
        raise NotImplementedError

    @async_and_sync
    @put_method(ROUTE_MODELS_STYLE)
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
    @get_method(ROUTE_MAP)
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
        """
        raise NotImplementedError

    @async_and_sync
    @put_method(ROUTE_MAP)
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
    @get_method(ROUTE_SCATTER)
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
        """
        raise NotImplementedError

    @async_and_sync
    @put_method(ROUTE_SCATTER)
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
    @get_method(ROUTE_PROFILES)
    async def get_profiles(
        self,
        project: str,
        experiment: str,
        location: str,
        network: str,
        obsvar: str,
        /,
        *args,
        **kwargs,
    ):
        """Fetches profiles from db.

        :param project: Project ID.
        :param experiment: Experiment ID.
        :param location: Location.
        :param network: Observation network.
        :param obsvar: Observation variable.
        """
        raise NotImplementedError

    @async_and_sync
    @put_method(ROUTE_PROFILES)
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
    @get_method(ROUTE_HEATMAP_TIMESERIES)
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
        **kwargs,
    ):
        """Fetches heatmap timeseries.

        :param project: Project ID.
        :param experiment: Experiment ID.
        :param region: Region ID.
        :param network: Observation Network.
        :param obsvar: Observation variable.
        :param layer: Layer.
        """
        raise NotImplementedError

    @async_and_sync
    @put_method(ROUTE_HEATMAP_TIMESERIES)
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
    @get_method(ROUTE_FORECAST)
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
        **kwargs,
    ):
        """Fetch forecast.

        :param project: Project ID.
        :param experiment: Experiment ID.
        :param region: Region ID.
        :param network: Observation Network.
        :param obsvar: Observation variable.
        :param layer: Layer.
        """
        raise NotImplementedError

    @async_and_sync
    @put_method(ROUTE_FORECAST)
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
    @get_method(ROUTE_GRIDDED_MAP)
    async def get_gridded_map(
        self, project: str, experiment: str, obsvar: str, model: str, /, *args, **kwargs
    ):
        """Fetches gridded map.

        :param project: Project ID.
        :param experiment: Experiment ID.
        :param obsvar: Observation variable.
        :param model: Model ID.
        """
        raise NotImplementedError

    @async_and_sync
    @put_method(ROUTE_GRIDDED_MAP)
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
    @get_method(ROUTE_REPORT)
    async def get_report(
        self, project: str, experiment: str, title: str, /, *args, **kwargs
    ):
        """Fetch report.

        :param project: Project ID.
        :param experiment: Experiment ID.
        :param title: Report title (ie. filename without extension).
        """
        raise NotImplementedError

    @async_and_sync
    @put_method(ROUTE_REPORT)
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
    async def get_by_uuid(
        self, uuid: str, /, access_type: str | AccessType, cache: bool = False
    ):
        """Gets a stored object by its UUID.

        :param uuid : uuid of the item to fetch.

        Note:
        -----
        UUID is implementation specific. While AerovalJsonFileDB returns
        a file path, this behaviour should not be relied upon as other
        implementations may not.
        """
        raise NotImplementedError

    @async_and_sync
    async def put_by_uuid(self, obj, uuid: str):
        """Replaces a stored object by uuid with a new object.

        :param obj: The object to be stored. Either a json str, or a
        json serializable python object.

        Note:
        -----
        UUID is implementation specific. While AerovalJsonFileDB returns
        a file path as the uuid, this behaviour should not be relied upon
        as other implementations will not.
        """
        raise NotImplementedError
