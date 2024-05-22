import abc
import functools
import inspect


def get_method(route):
    """Decorator for put functions, converts positional-only arguments into route_args

    :param route: the route for this method
    :param wrapped: function template, the wrapper function will never be called
    :return: getter function
    """

    def wrap(wrapped):
        @functools.wraps(wrapped)
        def wrapper(self, *args, **kwargs):
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

            print(route, route_args, *args, **kwargs)
            return self._get(route, route_args, *args, **kwargs)

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
        def wrapper(self, obj, *args, **kwargs):
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

            return self._put(obj, route, route_args, *args, **kwargs)

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

    def _get(self, route: str, route_args: dict[str, str], *args, **kwargs):
        """Abstract implementation of the main getter functions. All get and put
        functions map to this function, with a corresponding route as key
        to enable key/value pair put and get functionality.

        :param route: a route similar to a REST route
        :param route_args: route parameters, like format-dicts
        """
        raise NotImplementedError

    def _put(self, obj, route: str, route_args: dict[str, str], *args, **kwargs):
        """Abstract implementation of the main getter functions. All get and put
        functions map to this function, with a corresponding route as key
        to enable key/value pair put and get functionality.

        :param obj: the object to put
        :param route: a route similar to a REST route
        :param route_args: route parameters, like format-dicts
        """
        raise NotImplementedError

    @get_method("hm/ts/{place}_{component}")
    def get_heatmap_timeseries(self, place, component, /, *args, **kwargs):
        """Get a timeseries for a headmap and a place/region

        :param place: region for the timeseries
        :param component: timeseries component
        :raises NotImplementedError
        """
        raise NotImplementedError

    @put_method("hm/ts/{place}_{component}")
    def put_heatmap_timeseries(self, obj, place, component, /, *args, **kwargs):
        """Get a timeseries for a headmap and a place/region

        :param obj: the object to put
        :param place: region for the timeseries
        :param component: timeseries component
        :raises NotImplementedError
        """
        raise NotImplementedError

    @get_method("/glob_stats/{project}/{experiment}/{frequency}")
    def put_glob_stats(
        self, obj, project: str, experiment: str, frequency: str, /, *args, **kwargs
    ):
        raise NotImplementedError

    @put_method("/glob_stats/{project}/{experiment}/{frequency}")
    def get_glob_stats(
        self, project: str, experiment: str, frequency: str, /, *args, **kwargs
    ):
        raise NotImplementedError
